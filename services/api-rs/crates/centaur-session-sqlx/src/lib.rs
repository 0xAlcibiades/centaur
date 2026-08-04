//! SQLx-backed session repository.

use std::{collections::BTreeMap, str::FromStr, time::Duration};

use centaur_session_core::{
    ExecutionStatus, HarnessType, MessageRole, SandboxCapabilities, SandboxRepoCacheAccess,
    Session, SessionEvent, SessionExecution, SessionMessage, SessionMessageInput, SessionStatus,
    ThreadKey, empty_object,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
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

/// The exact backend identity of the sandbox assignment which owns a pipe.
/// A backend without stable resource UIDs still fences the assignment by epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxAssignmentIdentity {
    pub assignment_epoch: String,
    pub resource_uid: Option<String>,
}

/// Complete database snapshot used to fence an assignment write across the
/// external sandbox create/claim interval, including same-name ABA reuse.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxAssignmentSnapshot {
    pub sandbox_id: Option<String>,
    pub resource_uid: Option<String>,
    pub assignment_epoch: Option<String>,
}

impl SandboxAssignmentSnapshot {
    pub const fn unassigned() -> Self {
        Self {
            sandbox_id: None,
            resource_uid: None,
            assignment_epoch: None,
        }
    }
}

/// Immutable ingress payload persisted before a sandbox receives a stdin write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedInputDelivery {
    pub idempotency_key: String,
    pub message_ids: Vec<String>,
    pub input_lines: Vec<String>,
    pub boundary_fingerprint: String,
}

/// A server-generated message ID is prepared before entering the transaction
/// so the persisted message IDs and the input-delivery payload are identical.
#[derive(Clone, Debug)]
pub struct PreparedSessionMessage {
    pub message_id: String,
    pub input: SessionMessageInput,
}

/// Result of deciding, while holding the per-session lifecycle lock, whether
/// messages can be persisted without a sandbox-input delivery. Terminal
/// executions deliberately do not create an input obligation.
#[derive(Clone, Debug)]
pub enum AppendMessagesWithoutActiveExecution {
    Appended(Vec<String>),
    Active(SessionExecution),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionInputDelivery {
    pub delivery_id: String,
    pub thread_key: ThreadKey,
    pub execution_id: String,
    pub sequence: i64,
    pub idempotency_key: String,
    pub message_ids: Vec<String>,
    pub input_lines: Vec<String>,
    /// SHA-256 of the original exact input, retained after `input_lines` is
    /// scrubbed so terminal deliveries remain auditable without plaintext.
    pub input_sha256: String,
    pub input_line_count: i32,
    pub boundary_fingerprint: String,
    pub state: InputDeliveryState,
    pub owner_id: Option<String>,
    pub owner_generation: i64,
    pub owner_lease_expires_at: Option<OffsetDateTime>,
    pub sandbox_id: Option<String>,
    pub sandbox_resource_uid: Option<String>,
    pub sandbox_assignment_epoch: Option<String>,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub created_at: OffsetDateTime,
    pub claimed_at: Option<OffsetDateTime>,
    pub flushed_at: Option<OffsetDateTime>,
    pub failed_at: Option<OffsetDateTime>,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputDeliveryState {
    Pending,
    Claimed,
    Ambiguous,
    Flushed,
    Failed,
}

impl InputDeliveryState {
    const fn as_ref(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Claimed => "claimed",
            Self::Ambiguous => "ambiguous",
            Self::Flushed => "flushed",
            Self::Failed => "failed",
        }
    }
}

fn input_lines_sha256(input_lines: &[String]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"centaur.session-input-delivery.v1\\0");
    for input_line in input_lines {
        hasher.update(
            u64::try_from(input_line.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        hasher.update(input_line.as_bytes());
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn delivery_matches_prepared(
    delivery: &SessionInputDelivery,
    prepared: &PreparedInputDelivery,
) -> bool {
    let payload_matches = delivery.input_sha256 == input_lines_sha256(&prepared.input_lines)
        && delivery.input_line_count
            == i32::try_from(prepared.input_lines.len()).unwrap_or(i32::MAX);
    let plaintext_matches = match delivery.state {
        InputDeliveryState::Pending
        | InputDeliveryState::Claimed
        | InputDeliveryState::Ambiguous => delivery.input_lines == prepared.input_lines,
        InputDeliveryState::Flushed | InputDeliveryState::Failed => delivery.input_lines.is_empty(),
    };
    delivery.message_ids == prepared.message_ids
        && delivery.boundary_fingerprint == prepared.boundary_fingerprint
        && payload_matches
        && plaintext_matches
}

impl FromStr for InputDeliveryState {
    type Err = SessionStoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "claimed" => Ok(Self::Claimed),
            "ambiguous" => Ok(Self::Ambiguous),
            "flushed" => Ok(Self::Flushed),
            "failed" => Ok(Self::Failed),
            _ => Err(SessionStoreError::InvalidPersistedValue(value.to_owned())),
        }
    }
}

#[derive(Clone, Debug)]
pub struct CreatedExecutionInputDelivery {
    pub execution: SessionExecution,
    pub delivery: SessionInputDelivery,
    /// `false` means the durable, byte-for-byte-equivalent existing request
    /// won the idempotency race and is returned for recovery.
    pub created: bool,
}

#[derive(Clone, Debug)]
pub struct ClaimedInputDelivery {
    pub delivery: SessionInputDelivery,
    pub execution: SessionExecution,
}

/// Holds all durable fences through the bounded sandbox pipe flush.
pub struct InputDeliveryFlushGuard {
    transaction: Transaction<'static, sqlx::Postgres>,
    delivery_id: String,
    owner_id: String,
    owner_generation: i64,
    thread_key: String,
    execution_id: String,
    deadline: OffsetDateTime,
}

impl InputDeliveryFlushGuard {
    pub fn remaining(&self) -> Option<Duration> {
        let remaining = self.deadline - OffsetDateTime::now_utc();
        (remaining > TimeDuration::ZERO)
            .then(|| Duration::from_secs(u64::try_from(remaining.whole_seconds()).unwrap_or(0)))
    }

    /// Atomically makes the exact delivery durable and emits the replayable
    /// input-flushed event. A stale owner/generation cannot commit either.
    pub async fn commit(mut self) -> Result<Option<SessionEvent>, SessionStoreError> {
        let row = sqlx::query_as::<_, (String, String, i32)>(
            r#"
            update session_input_deliveries
            set state = 'flushed', input_lines = '[]'::jsonb,
                owner_id = null, owner_lease_expires_at = null,
                flushed_at = clock_timestamp(), updated_at = now()
            where delivery_id = $1 and state = 'claimed'
              and owner_id = $2 and owner_generation = $3
              and owner_lease_expires_at > clock_timestamp()
            returning delivery_id, input_sha256, input_line_count
            "#,
        )
        .bind(&self.delivery_id)
        .bind(&self.owner_id)
        .bind(self.owner_generation)
        .fetch_optional(&mut *self.transaction)
        .await?;
        let Some((delivery_id, input_sha256, input_line_count)) = row else {
            self.transaction.rollback().await?;
            return Ok(None);
        };
        let event = sqlx::query_as::<_, SessionEventRow>(
            r#"
            insert into session_events (thread_key, execution_id, event_type, payload)
            values (
                $1,
                $2,
                'session.input_flushed',
                jsonb_build_object(
                    'delivery_id', $3,
                    'input_sha256', $4,
                    'input_line_count', $5
                )
            )
            returning event_id, thread_key, execution_id, event_type, payload, created_at
            "#,
        )
        .bind(&self.thread_key)
        .bind(&self.execution_id)
        .bind(&delivery_id)
        .bind(&input_sha256)
        .bind(input_line_count)
        .fetch_one(&mut *self.transaction)
        .await?;
        let event = event.try_into()?;
        self.transaction.commit().await?;
        Ok(Some(event))
    }

    pub async fn rollback(self) -> Result<(), SessionStoreError> {
        self.transaction.rollback().await?;
        Ok(())
    }
}

async fn persist_prepared_messages(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    thread_key: &ThreadKey,
    messages: &[PreparedSessionMessage],
) -> Result<Vec<String>, SessionStoreError> {
    let mut ids = Vec::with_capacity(messages.len());
    for message in messages {
        let input = &message.input;
        let persisted = sqlx::query_as::<_, PersistedPreparedMessageRow>(
            r#"
            insert into session_messages
                (message_id, thread_key, client_message_id, role, parts, metadata)
            values ($1, $2, $3, $4, $5, $6)
            on conflict (thread_key, client_message_id)
                where client_message_id is not null
            do update set client_message_id = excluded.client_message_id
            returning message_id, role, parts, metadata
            "#,
        )
        .bind(&message.message_id)
        .bind(thread_key.as_str())
        .bind(input.client_message_id.as_deref())
        .bind(input.role.as_ref())
        .bind(Value::Array(input.parts.clone()))
        .bind(input.metadata.clone())
        .fetch_one(&mut **tx)
        .await?;
        if persisted.role != input.role.as_ref()
            || persisted.parts != Value::Array(input.parts.clone())
            || persisted.metadata != input.metadata
        {
            return Err(SessionStoreError::InputDeliveryIdempotencyConflict);
        }
        ids.push(persisted.message_id);
    }
    Ok(ids)
}

async fn fetch_input_delivery_by_idempotency(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    thread_key: &ThreadKey,
    idempotency_key: &str,
) -> Result<Option<SessionInputDelivery>, SessionStoreError> {
    let row = sqlx::query_as::<_, SessionInputDeliveryRow>(
        r#"
        select delivery_id, thread_key, execution_id, sequence, idempotency_key,
               message_ids, input_lines, input_sha256, input_line_count, boundary_fingerprint, state, owner_id,
               owner_generation, owner_lease_expires_at, sandbox_id,
               sandbox_resource_uid, sandbox_assignment_epoch, attempts, last_error,
               created_at, claimed_at, flushed_at, failed_at, updated_at
        from session_input_deliveries where thread_key = $1 and idempotency_key = $2
        for update
        "#,
    )
    .bind(thread_key.as_str())
    .bind(idempotency_key)
    .fetch_optional(&mut **tx)
    .await?;
    row.map(TryInto::try_into).transpose()
}

async fn insert_input_delivery(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    thread_key: &ThreadKey,
    execution_id: &str,
    sequence: i64,
    prepared: &PreparedInputDelivery,
) -> Result<SessionInputDelivery, SessionStoreError> {
    let row = sqlx::query_as::<_, SessionInputDeliveryRow>(
        r#"
        insert into session_input_deliveries
            (delivery_id, thread_key, execution_id, sequence, idempotency_key,
             message_ids, input_lines, input_sha256, input_line_count, boundary_fingerprint)
        values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        returning delivery_id, thread_key, execution_id, sequence, idempotency_key,
            message_ids, input_lines, input_sha256, input_line_count, boundary_fingerprint, state, owner_id,
            owner_generation, owner_lease_expires_at, sandbox_id,
            sandbox_resource_uid, sandbox_assignment_epoch, attempts, last_error,
            created_at, claimed_at, flushed_at, failed_at, updated_at
        "#,
    )
    .bind(prefixed_id("dly"))
    .bind(thread_key.as_str())
    .bind(execution_id)
    .bind(sequence)
    .bind(&prepared.idempotency_key)
    .bind(Json(&prepared.message_ids))
    .bind(Json(&prepared.input_lines))
    .bind(input_lines_sha256(&prepared.input_lines))
    .bind(i32::try_from(prepared.input_lines.len()).unwrap_or(i32::MAX))
    .bind(&prepared.boundary_fingerprint)
    .fetch_one(&mut **tx)
    .await?;
    row.try_into()
}

async fn lock_execution_input_delivery_lifecycle(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    execution_id: &str,
) -> Result<bool, SessionStoreError> {
    let thread_key = sqlx::query_scalar::<_, String>(
        "select thread_key from session_executions where execution_id = $1",
    )
    .bind(execution_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(thread_key) = thread_key else {
        return Ok(false);
    };
    sqlx::query("select thread_key from sessions where thread_key = $1 for update")
        .bind(&thread_key)
        .fetch_optional(&mut **tx)
        .await?;
    sqlx::query("select execution_id from session_executions where execution_id = $1 for update")
        .bind(execution_id)
        .fetch_optional(&mut **tx)
        .await?;
    sqlx::query(
        "select delivery_id from session_input_deliveries where execution_id = $1 order by sequence for update",
    )
    .bind(execution_id)
    .fetch_all(&mut **tx)
    .await?;
    Ok(true)
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
    pub resource_uid: Option<String>,
    pub assignment_epoch: Option<String>,
    pub execution_id: String,
    pub idle_timeout: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxCapacityCandidate {
    pub thread_key: ThreadKey,
    pub sandbox_id: String,
    pub resource_uid: Option<String>,
    pub assignment_epoch: Option<String>,
    pub latest_execution_id: Option<String>,
    pub last_active_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowOwnedSandbox {
    pub thread_key: ThreadKey,
    pub sandbox_id: String,
    pub resource_uid: Option<String>,
    pub assignment_epoch: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, sqlx::FromRow)]
pub struct ReadyWarmSandbox {
    pub sandbox_id: String,
    pub resource_uid: Option<String>,
    pub assignment_epoch: Option<String>,
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
    _resource_uid: String,
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
    pub assignment_epoch: String,
    pub resource_uid: String,
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
    resource_uid: String,
}

#[derive(sqlx::FromRow)]
struct SandboxAssignmentReconciliationLockRow {
    sandbox_id: Option<String>,
    sandbox_resource_uid: Option<String>,
    sandbox_assignment_epoch: Option<String>,
    sandbox_metadata_trace_assignment_epoch: Option<String>,
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

async fn metadata_trace_request_result_is_current(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    result: &MetadataTraceConsent,
) -> Result<bool, SessionStoreError> {
    let current = sqlx::query_as::<_, MetadataTraceConsentRow>(
        r#"
        select source, workspace_id, user_id, enabled, expires_at, revision, drain_pending
        from metadata_trace_consents
        where source = $1 and workspace_id = $2 and user_id = $3
        for share
        "#,
    )
    .bind(&result.source)
    .bind(&result.workspace_id)
    .bind(&result.user_id)
    .fetch_optional(&mut **tx)
    .await?
    .map(Into::into);
    let now = sqlx::query_scalar::<_, OffsetDateTime>("select clock_timestamp()")
        .fetch_one(&mut **tx)
        .await?;
    Ok(current.is_some_and(|current: MetadataTraceConsent| {
        current.enabled == result.enabled
            && current.expires_at == result.expires_at
            && current.revision == result.revision
            && current.drain_pending == result.drain_pending
            && (!current.enabled || current.expires_at.is_some_and(|expiry| expiry > now))
    }))
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
    expected_revision: Option<i64>,
) -> Result<MetadataTraceConsent, SessionStoreError> {
    let current = sqlx::query_as::<_, MetadataTraceConsentRow>(
        r#"
        select source, workspace_id, user_id, enabled, expires_at, revision, drain_pending
        from metadata_trace_consents
        where source = $1 and workspace_id = $2 and user_id = $3
        for update
        "#,
    )
    .bind(source)
    .bind(workspace_id)
    .bind(user_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(current) = current else {
        if expected_revision.is_some_and(|revision| revision != 0) {
            return Err(SessionStoreError::MetadataTraceConsentRevisionChanged);
        }
        return Ok(sqlx::query_as::<_, MetadataTraceConsentRow>(
            r#"
            insert into metadata_trace_consents
                (source, workspace_id, user_id, enabled, expires_at, revision, drain_pending)
            values ($1, $2, $3, true, $4, 1, false)
            returning source, workspace_id, user_id, enabled, expires_at, revision, drain_pending
            "#,
        )
        .bind(source)
        .bind(workspace_id)
        .bind(user_id)
        .bind(expires_at)
        .fetch_one(&mut **tx)
        .await?
        .into());
    };
    if current.drain_pending {
        return Err(SessionStoreError::MetadataTraceDrainPending);
    }
    if expected_revision.is_some_and(|revision| revision != current.revision) {
        return Err(SessionStoreError::MetadataTraceConsentRevisionChanged);
    }
    Ok(sqlx::query_as::<_, MetadataTraceConsentRow>(
        r#"
        update metadata_trace_consents
        set enabled = true,
            expires_at = $4,
            revision = case
                when enabled and expires_at = $4 then revision
                else revision + 1
            end,
            updated_at = now()
        where source = $1 and workspace_id = $2 and user_id = $3
        returning source, workspace_id, user_id, enabled, expires_at, revision, drain_pending
        "#,
    )
    .bind(source)
    .bind(workspace_id)
    .bind(user_id)
    .bind(expires_at)
    .fetch_one(&mut **tx)
    .await?
    .into())
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
    sandbox_resource_uid: Option<String>,
    sandbox_assignment_epoch: Option<String>,
    sandbox_metadata_trace_assignment_epoch: Option<String>,
}

impl SandboxAssignmentReconciliationLock<'_> {
    pub fn resource_uid(&self) -> Option<&str> {
        self.sandbox_resource_uid.as_deref()
    }

    pub fn assignment_epoch(&self) -> Option<&str> {
        self.sandbox_assignment_epoch.as_deref()
    }

    /// Fence a pre-identity assignment while its session row is locked. The
    /// observed backend UID becomes durable before cleanup, so any same-name
    /// replacement is distinguishable on the next reconciliation attempt.
    pub async fn initialize_legacy_identity(
        &mut self,
        resource_uid: &str,
    ) -> Result<bool, SessionStoreError> {
        if self.sandbox_resource_uid.is_some() && self.sandbox_assignment_epoch.is_some() {
            return Ok(false);
        }
        let assignment_epoch = sqlx::query_scalar::<_, String>(
            r#"
            update sessions
            set sandbox_resource_uid = $3,
                sandbox_assignment_epoch = md5(random()::text || clock_timestamp()::text),
                updated_at = now()
            where thread_key = $1
              and sandbox_id = $2
            returning sandbox_assignment_epoch
            "#,
        )
        .bind(&self.thread_key)
        .bind(&self.sandbox_id)
        .bind(resource_uid)
        .fetch_optional(&mut *self.transaction)
        .await?;
        let Some(assignment_epoch) = assignment_epoch else {
            return Ok(false);
        };
        self.sandbox_resource_uid = Some(resource_uid.to_owned());
        self.sandbox_assignment_epoch = Some(assignment_epoch);
        Ok(true)
    }

    /// Revalidate that the execution which scheduled an idle pause is still
    /// the latest terminal execution and that no successor is active. This
    /// runs inside the assignment-row transaction, so execution creation must
    /// serialize with the ensuing exact backend pause.
    pub async fn is_idle_after_execution(
        &mut self,
        execution_id: &str,
        idle_timeout: Duration,
    ) -> Result<bool, SessionStoreError> {
        let idle = sqlx::query_scalar::<_, bool>(
            r#"
            select exists (
                select 1
                from session_executions latest
                where latest.thread_key = $1
                  and latest.execution_id = $2
                  and latest.status in ('completed', 'failed', 'cancelled')
                  and latest.completed_at is not null
                  and latest.completed_at <= now() - ($3::float8 * interval '1 second')
                  and not exists (
                      select 1
                      from session_executions newer
                      where newer.thread_key = latest.thread_key
                        and (newer.created_at, newer.execution_id) > (latest.created_at, latest.execution_id)
                  )
                  and not exists (
                      select 1
                      from session_executions active
                      where active.thread_key = latest.thread_key
                        and active.status in ('queued', 'running')
                  )
            )
            "#,
        )
        .bind(&self.thread_key)
        .bind(execution_id)
        .bind(idle_timeout.as_secs_f64())
        .fetch_one(&mut *self.transaction)
        .await?;
        Ok(idle)
    }

    /// Revalidate a capacity candidate while the durable assignment is locked.
    pub async fn is_idle_without_active_execution(
        &mut self,
        hot_idle_grace: Duration,
    ) -> Result<bool, SessionStoreError> {
        let idle = sqlx::query_scalar::<_, bool>(
            r#"
            select exists (
                select 1
                from sessions session
                where session.thread_key = $1
                  and session.sandbox_id = $2
                  and session.sandbox_last_active_at is not null
                  and session.sandbox_last_active_at <= now() - ($3::float8 * interval '1 second')
                  and not exists (
                      select 1
                      from session_executions active
                      where active.thread_key = session.thread_key
                        and active.status in ('queued', 'running')
                  )
            )
            "#,
        )
        .bind(&self.thread_key)
        .bind(&self.sandbox_id)
        .bind(hot_idle_grace.as_secs_f64())
        .fetch_one(&mut *self.transaction)
        .await?;
        Ok(idle)
    }

    /// The trace assignment epoch identifies the trace capability boundary;
    /// it is distinct from the general sandbox assignment epoch used by the
    /// row-lock CAS.
    pub fn metadata_trace_assignment_epoch(&self) -> Option<&str> {
        self.sandbox_metadata_trace_assignment_epoch.as_deref()
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
                sandbox_metadata_trace_assignment_epoch = null,
                sandbox_resource_uid = null,
                sandbox_assignment_epoch = null,
                sandbox_last_active_at = null,
                updated_at = now()
            where thread_key = $1
              and sandbox_id = $2
              and sandbox_resource_uid is not distinct from $3
              and sandbox_assignment_epoch is not distinct from $4
            "#,
        )
        .bind(&self.thread_key)
        .bind(&self.sandbox_id)
        .bind(&self.sandbox_resource_uid)
        .bind(&self.sandbox_assignment_epoch)
        .execute(&mut *self.transaction)
        .await?;
        self.transaction.commit().await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn rollback(self) -> Result<(), SessionStoreError> {
        self.transaction.rollback().await?;
        Ok(())
    }

    /// Release the assignment lock only if the exact assignment is still
    /// current. Callers use this after a non-terminal backend transition
    /// (such as pause) before retiring the matching in-memory pipe.
    pub async fn commit_if_current(mut self) -> Result<bool, SessionStoreError> {
        let result = sqlx::query(
            r#"
            update sessions
            set updated_at = updated_at
            where thread_key = $1
              and sandbox_id = $2
              and sandbox_resource_uid is not distinct from $3
              and sandbox_assignment_epoch is not distinct from $4
            "#,
        )
        .bind(&self.thread_key)
        .bind(&self.sandbox_id)
        .bind(&self.sandbox_resource_uid)
        .bind(&self.sandbox_assignment_epoch)
        .execute(&mut *self.transaction)
        .await?;
        self.transaction.commit().await?;
        Ok(result.rows_affected() == 1)
    }

    /// Atomically retire this exact assignment while changing harnesses. The
    /// backend stop happens while this row lock is held, so a replacement
    /// cannot be cleared by a stale harness-restart completion.
    pub async fn switch_harness_and_commit(
        mut self,
        harness_type: &HarnessType,
    ) -> Result<Option<Session>, SessionStoreError> {
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
                sandbox_metadata_trace_assignment_epoch = null,
                sandbox_resource_uid = null,
                sandbox_assignment_epoch = null,
                sandbox_last_active_at = null,
                status = $3,
                updated_at = now()
            where thread_key = $1
              and sandbox_id = $4
              and sandbox_resource_uid is not distinct from $5
              and sandbox_assignment_epoch is not distinct from $6
            returning thread_key, title, sandbox_id, sandbox_repo_cache_enabled, sandbox_repo_cache_access, sandbox_observability_enabled, sandbox_api_server_enabled, sandbox_metadata_trace_enabled, sandbox_metadata_trace_expires_at, sandbox_metadata_trace_subject_hash, sandbox_metadata_trace_consent_revision, sandbox_metadata_trace_config_fingerprint, sandbox_metadata_trace_config_generation, harness_type, harness_thread_id, persona_id, status, iron_control_principal, proxy_labels, sandbox_last_active_at, created_at, updated_at
            "#,
        )
        .bind(&self.thread_key)
        .bind(harness_type.as_ref())
        .bind(SessionStatus::Idle.as_ref())
        .bind(&self.sandbox_id)
        .bind(&self.sandbox_resource_uid)
        .bind(&self.sandbox_assignment_epoch)
        .fetch_optional(&mut *self.transaction)
        .await?;
        self.transaction.commit().await?;
        row.map(TryInto::try_into).transpose()
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
        assignment_epoch: &str,
        resource_uid: &str,
    ) -> Result<Option<MetadataTraceInputGuard>, SessionStoreError> {
        let mut transaction = self.pool.begin().await?;
        set_metadata_trace_transaction_timeouts(&mut transaction).await?;
        let actor = sqlx::query_as::<_, MetadataTraceAssignmentActorRow>(
            r#"
            select s.sandbox_metadata_trace_source as source,
                   s.sandbox_metadata_trace_workspace_id as workspace_id,
                   s.sandbox_metadata_trace_user_id as user_id,
                   s.sandbox_metadata_trace_assignment_epoch as assignment_epoch,
                   s.sandbox_metadata_trace_resource_uid as resource_uid
            from sessions s join session_executions e on e.thread_key = s.thread_key
            where e.execution_id = $1
              and s.thread_key = $2
              and e.status in ('queued', 'running')
              and s.sandbox_id = $3
              and s.sandbox_metadata_trace_enabled is true
              and s.sandbox_metadata_trace_assignment_epoch is not null
              and s.sandbox_metadata_trace_assignment_epoch = $9
              and s.sandbox_metadata_trace_resource_uid = $10
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
        .bind(assignment_epoch)
        .bind(resource_uid)
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
            _resource_uid: actor.resource_uid,
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
        let mut consent = row.map(Into::into).unwrap_or_else(|| MetadataTraceConsent {
            source: source.to_owned(),
            workspace_id: workspace_id.to_owned(),
            user_id: user_id.to_owned(),
            enabled: false,
            expires_at: None,
            revision: 0,
            drain_pending: false,
        });
        // Expiry is a lease boundary, not a delayed background mutation. Keep
        // its revision for a safe renewal, but never report an elapsed grant
        // as active while the reconciler catches up.
        if consent.enabled
            && consent
                .expires_at
                .is_none_or(|expiry| expiry <= OffsetDateTime::now_utc())
        {
            consent.enabled = false;
            consent.expires_at = None;
        }
        Ok(consent)
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
                   sandbox_metadata_trace_assignment_epoch as assignment_epoch,
                   sandbox_metadata_trace_resource_uid as resource_uid
            from sessions
            where thread_key = $1
              and sandbox_id = $2
              and sandbox_metadata_trace_enabled is true
              and sandbox_metadata_trace_assignment_epoch is not null
              and sandbox_metadata_trace_source is not null
              and sandbox_metadata_trace_workspace_id is not null
              and sandbox_metadata_trace_user_id is not null
              and sandbox_metadata_trace_resource_uid is not null
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
            assignment_epoch: row.assignment_epoch,
            resource_uid: row.resource_uid,
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

    /// Expiry is a durable revoke boundary.  The sidecar has its own deadline
    /// fence, while this transition records exact drains even when a backend
    /// is unavailable during the first reconciliation attempt.
    pub async fn expire_elapsed_metadata_trace_consents(
        &self,
    ) -> Result<Vec<MetadataTraceConsent>, SessionStoreError> {
        let rows = sqlx::query_as::<_, MetadataTraceConsentRow>(
            r#"
            update metadata_trace_consents as consent
            set enabled = false,
                expires_at = null,
                revision = consent.revision + 1,
                drain_pending = exists (
                    select 1 from sessions
                    where sandbox_metadata_trace_enabled is true
                      and sandbox_metadata_trace_source = consent.source
                      and sandbox_metadata_trace_workspace_id = consent.workspace_id
                      and sandbox_metadata_trace_user_id = consent.user_id
                      and sandbox_metadata_trace_consent_revision = consent.revision
                      and sandbox_id is not null
                ),
                updated_at = now()
            where consent.enabled is true
              and consent.expires_at <= clock_timestamp()
            returning source, workspace_id, user_id, enabled, expires_at, revision, drain_pending
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Return the exact targets for a still-current disabled consent revision.
    /// A reconciler can retain a stale pending snapshot, so this check is the
    /// ABA fence between a completed drain and a later regrant.
    pub async fn metadata_trace_drain_targets_if_current(
        &self,
        source: &str,
        workspace_id: &str,
        user_id: &str,
        disabled_revision: i64,
    ) -> Result<Option<Vec<MetadataTraceDrainTarget>>, SessionStoreError> {
        let mut tx = self.pool.begin().await?;
        set_metadata_trace_transaction_timeouts(&mut tx).await?;
        let current = sqlx::query_scalar::<_, i64>(
            r#"
            select revision from metadata_trace_consents
            where source = $1 and workspace_id = $2 and user_id = $3
              and revision = $4 and enabled is false and drain_pending is true
            for update
            "#,
        )
        .bind(source)
        .bind(workspace_id)
        .bind(user_id)
        .bind(disabled_revision)
        .fetch_optional(&mut *tx)
        .await?;
        if current.is_none() {
            tx.commit().await?;
            return Ok(None);
        }
        let targets = match disabled_revision.checked_sub(1) {
            Some(assignment_revision) => {
                metadata_trace_drain_targets_in_transaction(
                    &mut tx,
                    source,
                    workspace_id,
                    user_id,
                    assignment_revision,
                )
                .await?
            }
            None => Vec::new(),
        };
        tx.commit().await?;
        Ok(Some(targets))
    }

    /// Atomically reserve, apply, and persist a grant result.  A request row
    /// with an incomplete result is never re-applied: it can only have been
    /// written by an older binary, because this method commits the reservation
    /// and result together with the consent mutation.
    #[allow(clippy::too_many_arguments)]
    pub async fn grant_metadata_trace_consent_idempotent(
        &self,
        source: &str,
        workspace_id: &str,
        user_id: &str,
        expires_at: OffsetDateTime,
        expected_revision: Option<i64>,
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
            if !metadata_trace_request_result_is_current(&mut tx, &result).await? {
                return Err(SessionStoreError::MetadataTraceIdempotencyReplayFenced);
            }
            tx.commit().await?;
            return Ok(result);
        }
        let consent = grant_metadata_trace_consent_in_transaction(
            &mut tx,
            source,
            workspace_id,
            user_id,
            expires_at,
            expected_revision,
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

    /// A deployment without an explicit disabled trace generation is safe
    /// only before this database has ever participated in metadata tracing.
    pub async fn has_persisted_metadata_trace_state(&self) -> Result<bool, SessionStoreError> {
        let exists = sqlx::query_scalar::<_, bool>(
            r#"
            select exists (select 1 from metadata_trace_config_state)
                or exists (select 1 from metadata_trace_consents)
                or exists (select 1 from metadata_trace_consent_requests)
                or exists (
                    select 1
                    from sessions
                    where sandbox_metadata_trace_enabled is true
                       or sandbox_metadata_trace_config_generation is not null
                       or sandbox_metadata_trace_assignment_epoch is not null
                       or sandbox_metadata_trace_resource_uid is not null
                )
            "#,
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(exists)
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

    /// Atomically decides whether a newly acknowledged message has an active
    /// execution to steer. Both this decision and execution creation lock the
    /// session row, so a concurrent execution cannot start between a
    /// no-active observation and the durable message append.
    pub async fn append_prepared_messages_if_no_active_execution(
        &self,
        thread_key: &ThreadKey,
        messages: &[PreparedSessionMessage],
    ) -> Result<AppendMessagesWithoutActiveExecution, SessionStoreError> {
        let mut tx = self.pool.begin().await?;
        let session = sqlx::query_scalar::<_, String>(
            "select thread_key from sessions where thread_key = $1 for update",
        )
        .bind(thread_key.as_str())
        .fetch_optional(&mut *tx)
        .await?;
        if session.is_none() {
            tx.rollback().await?;
            return Err(SessionStoreError::NotFound {
                thread_key: thread_key.as_str().to_owned(),
            });
        }
        let active = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            select execution_id, idempotency_key, thread_key, status, metadata, error,
                   created_at, updated_at, started_at, completed_at
            from session_executions
            where thread_key = $1 and status in ('queued', 'running')
            order by created_at desc, execution_id desc
            limit 1
            for update
            "#,
        )
        .bind(thread_key.as_str())
        .fetch_optional(&mut *tx)
        .await?;
        let outcome = if let Some(active) = active {
            AppendMessagesWithoutActiveExecution::Active(active.try_into()?)
        } else {
            AppendMessagesWithoutActiveExecution::Appended(
                persist_prepared_messages(&mut tx, thread_key, messages).await?,
            )
        };
        tx.commit().await?;
        Ok(outcome)
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
        let mut tx = self.pool.begin().await?;
        let session = sqlx::query_scalar::<_, String>(
            "select thread_key from sessions where thread_key = $1 for update",
        )
        .bind(thread_key.as_str())
        .fetch_optional(&mut *tx)
        .await?;
        if session.is_none() {
            tx.rollback().await?;
            return Err(SessionStoreError::NotFound {
                thread_key: thread_key.as_str().to_owned(),
            });
        }
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
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        row.try_into()
    }

    /// Creates the execution and its sequence-zero input obligation in the
    /// same transaction. Retrying an idempotency key returns only the original
    /// exact payload; a caller can never silently attach new input to an old
    /// execution.
    pub async fn create_execution_with_initial_input_delivery(
        &self,
        thread_key: &ThreadKey,
        execution_idempotency_key: &str,
        metadata: Value,
        prepared: &PreparedInputDelivery,
    ) -> Result<CreatedExecutionInputDelivery, SessionStoreError> {
        let execution_id = prefixed_id("exe");
        let delivery_id = prefixed_id("dly");
        let mut tx = self.pool.begin().await?;
        let session = sqlx::query_scalar::<_, String>(
            "select thread_key from sessions where thread_key = $1 for update",
        )
        .bind(thread_key.as_str())
        .fetch_optional(&mut *tx)
        .await?;
        if session.is_none() {
            tx.rollback().await?;
            return Err(SessionStoreError::NotFound {
                thread_key: thread_key.as_str().to_owned(),
            });
        }
        let execution = sqlx::query_as::<_, CreateExecutionRow>(
            r#"
            insert into session_executions
                (execution_id, thread_key, idempotency_key, status, metadata)
            values ($1, $2, $3, 'queued', $4)
            on conflict (thread_key, idempotency_key)
                where idempotency_key is not null
            do update set idempotency_key = excluded.idempotency_key
            returning execution_id = $1 as created, execution_id, idempotency_key,
                thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            "#,
        )
        .bind(&execution_id)
        .bind(thread_key.as_str())
        .bind(execution_idempotency_key)
        .bind(metadata)
        .fetch_one(&mut *tx)
        .await?;
        let existing = sqlx::query_as::<_, SessionInputDeliveryRow>(
            r#"
            select delivery_id, thread_key, execution_id, sequence, idempotency_key,
                   message_ids, input_lines, input_sha256, input_line_count, boundary_fingerprint, state, owner_id,
                   owner_generation, owner_lease_expires_at, sandbox_id,
                   sandbox_resource_uid, sandbox_assignment_epoch, attempts, last_error,
                   created_at, claimed_at, flushed_at, failed_at, updated_at
            from session_input_deliveries where thread_key = $1 and idempotency_key = $2
            for update
            "#,
        )
        .bind(thread_key.as_str())
        .bind(&prepared.idempotency_key)
        .fetch_optional(&mut *tx)
        .await?;
        let delivery = if let Some(existing) = existing {
            let delivery: SessionInputDelivery = existing.try_into()?;
            if delivery.execution_id != execution.execution_id
                || delivery.sequence != 0
                || !delivery_matches_prepared(&delivery, prepared)
            {
                tx.rollback().await?;
                return Err(SessionStoreError::InputDeliveryIdempotencyConflict);
            }
            delivery
        } else {
            if !execution.created {
                tx.rollback().await?;
                return Err(
                    SessionStoreError::InputDeliveryMissingForExistingExecution {
                        execution_id: execution.execution_id,
                    },
                );
            }
            let row = sqlx::query_as::<_, SessionInputDeliveryRow>(
                r#"
                insert into session_input_deliveries
                    (delivery_id, thread_key, execution_id, sequence, idempotency_key,
                     message_ids, input_lines, input_sha256, input_line_count, boundary_fingerprint)
                values ($1, $2, $3, 0, $4, $5, $6, $7, $8, $9)
                returning delivery_id, thread_key, execution_id, sequence, idempotency_key,
                    message_ids, input_lines, input_sha256, input_line_count, boundary_fingerprint, state, owner_id,
                    owner_generation, owner_lease_expires_at, sandbox_id,
                    sandbox_resource_uid, sandbox_assignment_epoch, attempts, last_error,
                    created_at, claimed_at, flushed_at, failed_at, updated_at
                "#,
            )
            .bind(&delivery_id)
            .bind(thread_key.as_str())
            .bind(&execution.execution_id)
            .bind(&prepared.idempotency_key)
            .bind(Json(&prepared.message_ids))
            .bind(Json(&prepared.input_lines))
            .bind(input_lines_sha256(&prepared.input_lines))
            .bind(i32::try_from(prepared.input_lines.len()).unwrap_or(i32::MAX))
            .bind(&prepared.boundary_fingerprint)
            .fetch_one(&mut *tx)
            .await?;
            row.try_into()?
        };
        let execution: CreateExecutionResult = execution.try_into()?;
        let created = execution.created;
        tx.commit().await?;
        Ok(CreatedExecutionInputDelivery {
            execution: execution.execution,
            delivery,
            created,
        })
    }

    pub async fn current_sandbox_assignment_identity(
        &self,
        thread_key: &ThreadKey,
        sandbox_id: &str,
    ) -> Result<Option<SandboxAssignmentIdentity>, SessionStoreError> {
        let row = sqlx::query_as::<_, SandboxAssignmentIdentityRow>(
            r#"
            select sandbox_assignment_epoch as assignment_epoch,
                   sandbox_resource_uid as resource_uid
            from sessions
            where thread_key = $1 and sandbox_id = $2
              and sandbox_assignment_epoch is not null
            "#,
        )
        .bind(thread_key.as_str())
        .bind(sandbox_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(Into::into))
    }

    pub async fn sandbox_assignment_snapshot(
        &self,
        thread_key: &ThreadKey,
    ) -> Result<SandboxAssignmentSnapshot, SessionStoreError> {
        let row = sqlx::query_as::<_, SandboxAssignmentSnapshotRow>(
            r#"
            select sandbox_id, sandbox_resource_uid as resource_uid,
                   sandbox_assignment_epoch as assignment_epoch
            from sessions where thread_key = $1
            "#,
        )
        .bind(thread_key.as_str())
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| SessionStoreError::NotFound {
            thread_key: thread_key.as_str().to_owned(),
        })?;
        Ok(row.into())
    }

    /// Initializes the assignment fence for a legacy assignment only while it
    /// still names the observed sandbox. A different resource UID is rejected.
    pub async fn ensure_current_sandbox_assignment_identity(
        &self,
        thread_key: &ThreadKey,
        sandbox_id: &str,
        resource_uid: Option<&str>,
    ) -> Result<Option<SandboxAssignmentIdentity>, SessionStoreError> {
        let mut tx = self.pool.begin().await?;
        let initialized = sqlx::query_as::<_, SandboxAssignmentIdentityRow>(
            r#"
            update sessions
            set sandbox_resource_uid = $3,
                sandbox_assignment_epoch = md5(random()::text || clock_timestamp()::text),
                updated_at = now()
            where thread_key = $1 and sandbox_id = $2
              and sandbox_assignment_epoch is null
              and (sandbox_resource_uid is null
                   or sandbox_resource_uid is not distinct from $3)
            returning sandbox_assignment_epoch as assignment_epoch,
                      sandbox_resource_uid as resource_uid
            "#,
        )
        .bind(thread_key.as_str())
        .bind(sandbox_id)
        .bind(resource_uid)
        .fetch_optional(&mut *tx)
        .await?;
        let row = match initialized {
            Some(row) => Some(row),
            None => {
                sqlx::query_as::<_, SandboxAssignmentIdentityRow>(
                    r#"
                select sandbox_assignment_epoch as assignment_epoch,
                       sandbox_resource_uid as resource_uid
                from sessions
                where thread_key = $1 and sandbox_id = $2
                  and sandbox_assignment_epoch is not null
                  and sandbox_resource_uid is not distinct from $3
                "#,
                )
                .bind(thread_key.as_str())
                .bind(sandbox_id)
                .bind(resource_uid)
                .fetch_optional(&mut *tx)
                .await?
            }
        };
        tx.commit().await?;
        Ok(row.map(Into::into))
    }

    /// Claims the lowest recoverable input obligation. Supplying an execution
    /// or delivery target lets the foreground driver recover its own request;
    /// recovery workers leave both `None` and receive the global oldest row.
    pub async fn claim_next_input_delivery(
        &self,
        owner_id: &str,
        lease: Duration,
        execution_id: Option<&str>,
        delivery_id: Option<&str>,
    ) -> Result<Option<ClaimedInputDelivery>, SessionStoreError> {
        let mut tx = self.pool.begin().await?;
        let candidate = sqlx::query_as::<_, InputDeliveryCandidateRow>(
            r#"
            select d.delivery_id, d.execution_id
            from session_input_deliveries d
            join session_executions e on e.execution_id = d.execution_id
            where ($1::text is null or d.execution_id = $1)
              and ($2::text is null or d.delivery_id = $2)
              and e.status in ('queued', 'running')
              and (e.stdout_owner_id is null
                   or e.stdout_owner_lease_expires_at is null
                   or e.stdout_owner_lease_expires_at <= clock_timestamp()
                   or (e.stdout_owner_id = $3
                       and e.stdout_owner_lease_expires_at > clock_timestamp()))
              and (d.state in ('pending', 'ambiguous')
                   or (d.state = 'claimed' and d.owner_lease_expires_at <= clock_timestamp()))
              and not exists (
                    select 1 from session_input_deliveries earlier
                    where earlier.execution_id = d.execution_id
                      and earlier.sequence < d.sequence
                      and earlier.state in ('pending', 'claimed', 'ambiguous')
              )
            order by d.created_at, d.delivery_id
            for update of d, e skip locked
            limit 1
            "#,
        )
        .bind(execution_id)
        .bind(delivery_id)
        .bind(owner_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(candidate) = candidate else {
            tx.commit().await?;
            return Ok(None);
        };
        let execution = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            update session_executions
            set status = 'running', started_at = coalesce(started_at, clock_timestamp()),
                stdout_owner_id = $2,
                stdout_owner_lease_expires_at = clock_timestamp() + ($3::float8 * interval '1 second'),
                updated_at = now()
            where execution_id = $1 and status in ('queued', 'running')
              and (stdout_owner_id is null or stdout_owner_lease_expires_at is null
                   or stdout_owner_lease_expires_at <= clock_timestamp()
                   or (stdout_owner_id = $2
                       and stdout_owner_lease_expires_at > clock_timestamp()))
            returning execution_id, idempotency_key, thread_key, status, metadata, error,
                created_at, updated_at, started_at, completed_at
            "#,
        )
        .bind(&candidate.execution_id)
        .bind(owner_id)
        .bind(lease.as_secs_f64())
        .fetch_optional(&mut *tx)
        .await?;
        let Some(execution) = execution else {
            tx.rollback().await?;
            return Ok(None);
        };
        let delivery = sqlx::query_as::<_, SessionInputDeliveryRow>(
            r#"
            update session_input_deliveries
            set state = 'claimed', owner_id = $2, owner_generation = owner_generation + 1,
                owner_lease_expires_at = clock_timestamp() + ($3::float8 * interval '1 second'),
                attempts = attempts + 1, claimed_at = clock_timestamp(), updated_at = now()
            where delivery_id = $1
              and (state in ('pending', 'ambiguous')
                   or (state = 'claimed' and owner_lease_expires_at <= clock_timestamp()))
            returning delivery_id, thread_key, execution_id, sequence, idempotency_key,
                message_ids, input_lines, input_sha256, input_line_count, boundary_fingerprint, state, owner_id,
                owner_generation, owner_lease_expires_at, sandbox_id,
                sandbox_resource_uid, sandbox_assignment_epoch, attempts, last_error,
                created_at, claimed_at, flushed_at, failed_at, updated_at
            "#,
        )
        .bind(&candidate.delivery_id)
        .bind(owner_id)
        .bind(lease.as_secs_f64())
        .fetch_optional(&mut *tx)
        .await?;
        let Some(delivery) = delivery else {
            tx.rollback().await?;
            return Ok(None);
        };
        let execution = execution.try_into()?;
        let delivery = delivery.try_into()?;
        tx.commit().await?;
        Ok(Some(ClaimedInputDelivery {
            delivery,
            execution,
        }))
    }

    /// Locks the durable delivery protocol in the same order as consent and
    /// config mutation: consent, config, session, execution, delivery.
    #[allow(clippy::too_many_arguments)]
    pub async fn begin_input_delivery_flush(
        &self,
        delivery_id: &str,
        owner_id: &str,
        owner_generation: i64,
        sandbox_id: &str,
        expected: &SandboxCapabilities,
        boundary_fingerprint: &str,
        assignment: &SandboxAssignmentIdentity,
    ) -> Result<Option<InputDeliveryFlushGuard>, SessionStoreError> {
        let mut tx = self.pool.begin().await?;
        set_metadata_trace_transaction_timeouts(&mut tx).await?;
        let deadline = OffsetDateTime::now_utc() + TimeDuration::seconds(30);
        let consent = if expected.metadata_trace_enabled {
            let actor = sqlx::query_as::<_, MetadataTraceAssignmentActorRow>(
                r#"
                select s.sandbox_metadata_trace_source as source,
                       s.sandbox_metadata_trace_workspace_id as workspace_id,
                       s.sandbox_metadata_trace_user_id as user_id,
                       s.sandbox_metadata_trace_assignment_epoch as assignment_epoch,
                       s.sandbox_metadata_trace_resource_uid as resource_uid
                from sessions s join session_input_deliveries d on d.thread_key = s.thread_key
                where d.delivery_id = $1
                "#,
            )
            .bind(delivery_id)
            .fetch_optional(&mut *tx)
            .await?;
            let Some(actor) = actor else {
                tx.rollback().await?;
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
            .fetch_optional(&mut *tx)
            .await?;
            let config_active = sqlx::query_scalar::<_, bool>(
                "select generation = $1 and config_fingerprint = $2 from metadata_trace_config_state where singleton = true for share",
            )
            .bind(expected.metadata_trace_config_generation)
            .bind(&expected.metadata_trace_config_fingerprint)
            .fetch_optional(&mut *tx)
            .await?;
            if config_active != Some(true) {
                tx.rollback().await?;
                return Ok(None);
            }
            consent
        } else {
            None
        };
        let session = sqlx::query_as::<_, FlushSessionRow>(
            r#"
            select s.thread_key, s.sandbox_id, s.sandbox_resource_uid, s.sandbox_assignment_epoch,
                   s.sandbox_metadata_trace_enabled, s.sandbox_metadata_trace_expires_at,
                   s.sandbox_metadata_trace_subject_hash, s.sandbox_metadata_trace_consent_revision,
                   s.sandbox_metadata_trace_config_fingerprint, s.sandbox_metadata_trace_config_generation
            from sessions s join session_input_deliveries d on d.thread_key = s.thread_key
            where d.delivery_id = $1 for update of s
            "#,
        )
        .bind(delivery_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(session) = session else {
            tx.rollback().await?;
            return Ok(None);
        };
        let execution = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            select e.execution_id, e.idempotency_key, e.thread_key, e.status, e.metadata, e.error,
                   e.created_at, e.updated_at, e.started_at, e.completed_at
            from session_executions e join session_input_deliveries d on d.execution_id = e.execution_id
            where d.delivery_id = $1 for update of e
            "#,
        )
        .bind(delivery_id)
        .fetch_optional(&mut *tx)
        .await?;
        let delivery = sqlx::query_as::<_, SessionInputDeliveryRow>(
            r#"
            select delivery_id, thread_key, execution_id, sequence, idempotency_key,
                   message_ids, input_lines, input_sha256, input_line_count, boundary_fingerprint, state, owner_id,
                   owner_generation, owner_lease_expires_at, sandbox_id,
                   sandbox_resource_uid, sandbox_assignment_epoch, attempts, last_error,
                   created_at, claimed_at, flushed_at, failed_at, updated_at
            from session_input_deliveries where delivery_id = $1 for update
            "#,
        )
        .bind(delivery_id)
        .fetch_optional(&mut *tx)
        .await?;
        let (Some(execution), Some(delivery)) = (execution, delivery) else {
            tx.rollback().await?;
            return Ok(None);
        };
        let trace_matches = session.sandbox_metadata_trace_enabled
            == Some(expected.metadata_trace_enabled)
            && session.sandbox_metadata_trace_expires_at == expected.metadata_trace_expires_at
            && session.sandbox_metadata_trace_subject_hash == expected.metadata_trace_subject_hash
            && session.sandbox_metadata_trace_consent_revision
                == expected.metadata_trace_consent_revision
            && session.sandbox_metadata_trace_config_fingerprint
                == expected.metadata_trace_config_fingerprint
            && session.sandbox_metadata_trace_config_generation
                == expected.metadata_trace_config_generation;
        let assignment_matches = session.sandbox_id.as_deref() == Some(sandbox_id)
            && session.sandbox_assignment_epoch.as_deref() == Some(&assignment.assignment_epoch)
            && session.sandbox_resource_uid == assignment.resource_uid;
        let valid = execution.status == ExecutionStatus::Running.as_ref()
            && delivery.state == InputDeliveryState::Claimed.as_ref()
            && delivery.owner_id.as_deref() == Some(owner_id)
            && delivery.owner_generation == owner_generation
            && delivery
                .owner_lease_expires_at
                .is_some_and(|lease| lease > OffsetDateTime::now_utc())
            && delivery.boundary_fingerprint == boundary_fingerprint
            && assignment_matches
            && trace_matches;
        let consent_deadline = consent.and_then(|consent| {
            (consent.enabled
                && !consent.drain_pending
                && consent.revision == expected.metadata_trace_consent_revision.unwrap_or_default()
                && consent.expires_at == expected.metadata_trace_expires_at
                && consent
                    .expires_at
                    .is_some_and(|expiry| expiry > OffsetDateTime::now_utc()))
            .then_some(consent.expires_at)
            .flatten()
        });
        let deadline = consent_deadline
            .map(|value| value.min(deadline))
            .unwrap_or(deadline);
        if !valid || (expected.metadata_trace_enabled && consent_deadline.is_none()) {
            tx.rollback().await?;
            return Ok(None);
        }
        sqlx::query(
            r#"
            update session_input_deliveries
            set sandbox_id = $2, sandbox_resource_uid = $3, sandbox_assignment_epoch = $4,
                updated_at = now()
            where delivery_id = $1
            "#,
        )
        .bind(delivery_id)
        .bind(&session.sandbox_id)
        .bind(&session.sandbox_resource_uid)
        .bind(&session.sandbox_assignment_epoch)
        .execute(&mut *tx)
        .await?;
        Ok(Some(InputDeliveryFlushGuard {
            transaction: tx,
            delivery_id: delivery_id.to_owned(),
            owner_id: owner_id.to_owned(),
            owner_generation,
            thread_key: session.thread_key,
            execution_id: execution.execution_id,
            deadline,
        }))
    }

    pub async fn mark_input_delivery_ambiguous(
        &self,
        delivery_id: &str,
        owner_id: &str,
        owner_generation: i64,
        error: &str,
    ) -> Result<bool, SessionStoreError> {
        let result = sqlx::query(
            r#"
            update session_input_deliveries
            set state = 'ambiguous', owner_id = null, owner_lease_expires_at = null,
                last_error = $4, updated_at = now()
            where delivery_id = $1 and state = 'claimed' and owner_id = $2
              and owner_generation = $3
              and owner_lease_expires_at > clock_timestamp()
            "#,
        )
        .bind(delivery_id)
        .bind(owner_id)
        .bind(owner_generation)
        .bind(error)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Rebinds an unsent claimed delivery to the actor's current trace
    /// boundary. The caller must retire the previous exact sandbox assignment
    /// before this CAS; payload and ordering fields remain immutable.
    pub async fn rebind_claimed_input_delivery_boundary(
        &self,
        delivery_id: &str,
        owner_id: &str,
        owner_generation: i64,
        boundary_fingerprint: &str,
        execution_boundary: Value,
    ) -> Result<bool, SessionStoreError> {
        let mut tx = self.pool.begin().await?;
        let execution_id = sqlx::query_scalar::<_, String>(
            "select execution_id from session_input_deliveries where delivery_id = $1",
        )
        .bind(delivery_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(execution_id) = execution_id else {
            tx.rollback().await?;
            return Ok(false);
        };
        if !lock_execution_input_delivery_lifecycle(&mut tx, &execution_id).await? {
            tx.rollback().await?;
            return Ok(false);
        }
        let rebound = sqlx::query_scalar::<_, String>(
            r#"
            update session_input_deliveries
            set boundary_fingerprint = $4, updated_at = now()
            where delivery_id = $1 and state = 'claimed' and owner_id = $2
              and owner_generation = $3
              and owner_lease_expires_at > clock_timestamp()
            returning execution_id
            "#,
        )
        .bind(delivery_id)
        .bind(owner_id)
        .bind(owner_generation)
        .bind(boundary_fingerprint)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(rebound_execution_id) = rebound else {
            tx.rollback().await?;
            return Ok(false);
        };
        debug_assert_eq!(rebound_execution_id, execution_id);
        let execution = sqlx::query(
            "update session_executions set metadata = metadata || $2, updated_at = now() where execution_id = $1 and status = 'running'",
        )
        .bind(&execution_id)
        .bind(execution_boundary)
        .execute(&mut *tx)
        .await?;
        if execution.rows_affected() != 1 {
            tx.rollback().await?;
            return Ok(false);
        }
        tx.commit().await?;
        Ok(true)
    }

    /// Records a non-retryable local failure. This is deliberately separate
    /// from `ambiguous`: only an explicit failure disposition permits the
    /// execution's terminal failure path to proceed.
    pub async fn mark_input_delivery_failed(
        &self,
        delivery_id: &str,
        owner_id: &str,
        owner_generation: i64,
        error: &str,
    ) -> Result<bool, SessionStoreError> {
        let result = sqlx::query(
            r#"
            update session_input_deliveries
            set state = 'failed', input_lines = '[]'::jsonb,
                owner_id = null, owner_lease_expires_at = null,
                last_error = $4, failed_at = clock_timestamp(), updated_at = now()
            where delivery_id = $1 and state = 'claimed' and owner_id = $2
              and owner_generation = $3
              and owner_lease_expires_at > clock_timestamp()
            "#,
        )
        .bind(delivery_id)
        .bind(owner_id)
        .bind(owner_generation)
        .bind(error)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn list_unresolved_input_deliveries(
        &self,
    ) -> Result<Vec<SessionInputDelivery>, SessionStoreError> {
        self.list_input_deliveries_where("state in ('pending', 'claimed', 'ambiguous')")
            .await
    }

    pub async fn list_recoverable_input_deliveries(
        &self,
    ) -> Result<Vec<SessionInputDelivery>, SessionStoreError> {
        self.list_input_deliveries_where(
            "state in ('pending', 'ambiguous') or (state = 'claimed' and owner_lease_expires_at <= clock_timestamp())",
        )
        .await
    }

    async fn list_input_deliveries_where(
        &self,
        predicate: &str,
    ) -> Result<Vec<SessionInputDelivery>, SessionStoreError> {
        let sql = format!(
            "select delivery_id, thread_key, execution_id, sequence, idempotency_key, \
             message_ids, input_lines, input_sha256, input_line_count, boundary_fingerprint, state, owner_id, owner_generation, \
             owner_lease_expires_at, sandbox_id, sandbox_resource_uid, sandbox_assignment_epoch, \
             attempts, last_error, created_at, claimed_at, flushed_at, failed_at, updated_at \
             from session_input_deliveries where {predicate} order by created_at, delivery_id"
        );
        let rows = sqlx::query_as::<_, SessionInputDeliveryRow>(&sql)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(TryInto::try_into).collect()
    }

    /// Persists the exact messages and appends one delivery under the execution
    /// row lock, so neither a message acknowledgement nor a delivery can
    /// commit alone. The caller must use the returned original delivery on an
    /// idempotency replay instead of resending a newly prepared payload.
    pub async fn append_messages_and_enqueue_input_delivery(
        &self,
        thread_key: &ThreadKey,
        execution_id: &str,
        messages: &[PreparedSessionMessage],
        prepared: &PreparedInputDelivery,
    ) -> Result<Option<SessionInputDelivery>, SessionStoreError> {
        let mut tx = self.pool.begin().await?;
        let execution = sqlx::query_scalar::<_, String>(
            r#"
            select execution_id from session_executions
            where execution_id = $1 and thread_key = $2 and status in ('queued', 'running')
            for update
            "#,
        )
        .bind(execution_id)
        .bind(thread_key.as_str())
        .fetch_optional(&mut *tx)
        .await?;
        let Some(execution) = execution else {
            tx.commit().await?;
            return Ok(None);
        };
        let existing =
            fetch_input_delivery_by_idempotency(&mut tx, thread_key, &prepared.idempotency_key)
                .await?;
        if let Some(existing) = existing {
            if existing.execution_id != execution || !delivery_matches_prepared(&existing, prepared)
            {
                tx.rollback().await?;
                return Err(SessionStoreError::InputDeliveryIdempotencyConflict);
            }
            tx.commit().await?;
            return Ok(Some(existing));
        }
        let persisted_ids = persist_prepared_messages(&mut tx, thread_key, messages).await?;
        if persisted_ids != prepared.message_ids {
            tx.rollback().await?;
            return Err(SessionStoreError::InputDeliveryIdempotencyConflict);
        }
        let sequence = sqlx::query_scalar::<_, i64>(
            "select coalesce(max(sequence) + 1, 0) from session_input_deliveries where execution_id = $1",
        )
        .bind(&execution)
        .fetch_one(&mut *tx)
        .await?;
        let delivery =
            insert_input_delivery(&mut tx, thread_key, &execution, sequence, prepared).await?;
        tx.commit().await?;
        Ok(Some(delivery))
    }

    /// Replaces one active execution and persists the successor's first user
    /// messages plus sequence-zero delivery in one transaction.
    pub async fn replace_active_execution_with_initial_input_delivery(
        &self,
        execution_id: &str,
        messages: &[PreparedSessionMessage],
        successor_metadata: Value,
        terminal: OwnedTerminalEvent,
        prepared: &PreparedInputDelivery,
    ) -> Result<Option<(SessionExecution, SessionExecution, SessionInputDelivery)>, SessionStoreError>
    {
        let (terminal_status, _session_status, error, event_type, payload) = terminal.into_parts();
        let successor_id = prefixed_id("exe");
        let mut tx = self.pool.begin().await?;
        if !lock_execution_input_delivery_lifecycle(&mut tx, execution_id).await? {
            tx.commit().await?;
            return Ok(None);
        }
        let existing_old = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            select execution_id, idempotency_key, thread_key, status, metadata, error,
                   created_at, updated_at, started_at, completed_at
            from session_executions where execution_id = $1 for update
            "#,
        )
        .bind(execution_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(existing_old) = existing_old else {
            tx.commit().await?;
            return Ok(None);
        };
        let existing_thread: ThreadKey = parse_persisted(existing_old.thread_key.clone())?;
        if let Some(delivery) = fetch_input_delivery_by_idempotency(
            &mut tx,
            &existing_thread,
            &prepared.idempotency_key,
        )
        .await?
        {
            if !delivery_matches_prepared(&delivery, prepared) {
                tx.rollback().await?;
                return Err(SessionStoreError::InputDeliveryIdempotencyConflict);
            }
            let successor = sqlx::query_as::<_, SessionExecutionRow>(
                r#"
                select execution_id, idempotency_key, thread_key, status, metadata, error,
                       created_at, updated_at, started_at, completed_at
                from session_executions where execution_id = $1
                "#,
            )
            .bind(&delivery.execution_id)
            .fetch_one(&mut *tx)
            .await?;
            let old = existing_old.try_into()?;
            let successor = successor.try_into()?;
            tx.commit().await?;
            return Ok(Some((old, successor, delivery)));
        }
        let old = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            update session_executions
            set status = $2, error = $3, completed_at = coalesce(completed_at, now()),
                stdout_owner_id = null, stdout_owner_lease_expires_at = null, updated_at = now()
            where execution_id = $1 and status in ('queued', 'running')
              and not exists (
                    select 1 from session_input_deliveries delivery
                    where delivery.execution_id = session_executions.execution_id
                      and delivery.state in ('pending', 'claimed', 'ambiguous')
              )
            returning execution_id, idempotency_key, thread_key, status, metadata, error,
                created_at, updated_at, started_at, completed_at
            "#,
        )
        .bind(execution_id)
        .bind(terminal_status.as_ref())
        .bind(&error)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(old) = old else {
            tx.commit().await?;
            return Ok(None);
        };
        let thread_key: ThreadKey = parse_persisted(old.thread_key.clone())?;
        let persisted_ids = persist_prepared_messages(&mut tx, &thread_key, messages).await?;
        if persisted_ids != prepared.message_ids {
            tx.rollback().await?;
            return Err(SessionStoreError::InputDeliveryIdempotencyConflict);
        }
        let successor = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            insert into session_executions (execution_id, thread_key, idempotency_key, status, metadata)
            values ($1, $2, $3, 'queued', $4)
            returning execution_id, idempotency_key, thread_key, status, metadata, error,
                created_at, updated_at, started_at, completed_at
            "#,
        )
        .bind(&successor_id)
        .bind(thread_key.as_str())
        .bind(&prepared.idempotency_key)
        .bind(successor_metadata)
        .fetch_one(&mut *tx)
        .await?;
        let delivery =
            insert_input_delivery(&mut tx, &thread_key, &successor_id, 0, prepared).await?;
        sqlx::query("update sessions set status = $2, updated_at = now() where thread_key = $1")
            .bind(thread_key.as_str())
            .bind(SessionStatus::Executing.as_ref())
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "insert into session_events (thread_key, execution_id, event_type, payload) values ($1, $2, $3, $4)",
        )
        .bind(thread_key.as_str())
        .bind(execution_id)
        .bind(event_type)
        .bind(payload)
        .execute(&mut *tx)
        .await?;
        let old = old.try_into()?;
        let successor = successor.try_into()?;
        tx.commit().await?;
        Ok(Some((old, successor, delivery)))
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
              and not exists (
                    select 1 from session_input_deliveries delivery
                    where delivery.execution_id = session_executions.execution_id
                      and delivery.state in ('pending', 'claimed', 'ambiguous')
              )
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
            "#,
        )
        .bind(execution_id)
        .bind(owner_id)
        .bind(lease_seconds)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() == 0 {
            tx.rollback().await?;
            return Ok(false);
        }
        sqlx::query(
            r#"
            update session_input_deliveries
            set owner_lease_expires_at = clock_timestamp() + ($3::float8 * interval '1 second'),
                updated_at = now()
            where execution_id = $1 and state = 'claimed' and owner_id = $2
              and owner_lease_expires_at > clock_timestamp()
            "#,
        )
        .bind(execution_id)
        .bind(owner_id)
        .bind(lease_seconds)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
    }

    pub async fn release_stdout_owner(
        &self,
        execution_id: &str,
        owner_id: &str,
    ) -> Result<bool, SessionStoreError> {
        let mut tx = self.pool.begin().await?;
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
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() > 0 {
            sqlx::query(
                r#"
                update session_input_deliveries
                set state = 'ambiguous', owner_id = null, owner_lease_expires_at = null,
                    last_error = 'stdout owner released before input flush committed', updated_at = now()
                where execution_id = $1 and state = 'claimed' and owner_id = $2
                "#,
            )
            .bind(execution_id)
            .bind(owner_id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
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
        let mut tx = self.pool.begin().await?;
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
        .fetch_all(&mut *tx)
        .await?;
        sqlx::query(
            r#"
            update session_input_deliveries
            set state = 'ambiguous', owner_id = null, owner_lease_expires_at = null,
                last_error = 'control plane shut down before input flush committed', updated_at = now()
            where state = 'claimed' and owner_id = $1
            "#,
        )
        .bind(owner_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

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
              and not exists (
                    select 1 from session_input_deliveries delivery
                    where delivery.execution_id = session_executions.execution_id
                      and delivery.state in ('pending', 'claimed', 'ambiguous')
              )
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
              and not exists (
                    select 1 from session_input_deliveries delivery
                    where delivery.execution_id = session_executions.execution_id
                      and delivery.state in ('pending', 'claimed', 'ambiguous')
              )
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
              and not exists (
                    select 1 from session_input_deliveries delivery
                    where delivery.execution_id = session_executions.execution_id
                      and delivery.state in ('pending', 'claimed', 'ambiguous')
              )
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
              and not exists (
                    select 1 from session_input_deliveries delivery
                    where delivery.execution_id = session_executions.execution_id
                      and delivery.state in ('pending', 'claimed', 'ambiguous')
              )
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
        if !lock_execution_input_delivery_lifecycle(&mut tx, execution_id).await? {
            tx.commit().await?;
            return Ok(None);
        }
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
              and not exists (
                    select 1 from session_input_deliveries delivery
                    where delivery.execution_id = session_executions.execution_id
                      and delivery.state in ('pending', 'claimed', 'ambiguous')
              )
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

    /// Atomically terminalize whichever active owner currently holds an
    /// execution.  Boundary replacement uses this CAS instead of assuming the
    /// local stdout lease is current: a successor is safe only after this
    /// transaction has actually removed the one-active-execution row.
    pub async fn terminalize_execution_and_append_event_if_active(
        &self,
        execution_id: &str,
        terminal: OwnedTerminalEvent,
    ) -> Result<Option<(SessionExecution, SessionEvent)>, SessionStoreError> {
        let (terminal_status, session_status, error, event_type, payload) = terminal.into_parts();
        let mut tx = self.pool.begin().await?;
        if !lock_execution_input_delivery_lifecycle(&mut tx, execution_id).await? {
            tx.commit().await?;
            return Ok(None);
        }
        let execution_row = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            update session_executions
            set status = $2, error = $3, completed_at = coalesce(completed_at, now()),
                stdout_owner_id = null, stdout_owner_lease_expires_at = null, updated_at = now()
            where execution_id = $1 and status in ($4, $5)
              and not exists (
                    select 1 from session_input_deliveries delivery
                    where delivery.execution_id = session_executions.execution_id
                      and delivery.state in ('pending', 'claimed', 'ambiguous')
              )
            returning execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            "#,
        )
        .bind(execution_id)
        .bind(terminal_status.as_ref())
        .bind(&error)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .fetch_optional(&mut *tx)
        .await?;
        let Some(execution_row) = execution_row else {
            tx.commit().await?;
            return Ok(None);
        };
        let thread_key = execution_row.thread_key.clone();
        sqlx::query(
            r#"
            update sessions as s set status = $2, updated_at = now()
            where s.thread_key = $1 and not exists (
                select 1 from session_executions active
                where active.thread_key = s.thread_key and active.status in ($3, $4)
            )
            "#,
        )
        .bind(&thread_key)
        .bind(session_status.as_ref())
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .execute(&mut *tx)
        .await?;
        let event_row = sqlx::query_as::<_, SessionEventRow>(
            r#"
            insert into session_events (thread_key, execution_id, event_type, payload)
            values ($1, $2, $3, $4)
            returning event_id, thread_key, execution_id, event_type, payload, created_at
            "#,
        )
        .bind(&thread_key)
        .bind(execution_id)
        .bind(event_type)
        .bind(payload)
        .fetch_one(&mut *tx)
        .await?;
        let execution = execution_row.try_into()?;
        let event = event_row.try_into()?;
        tx.commit().await?;
        Ok(Some((execution, event)))
    }

    /// Replace an active execution with a queued successor in one transaction.
    /// The partial active-execution index therefore never exposes a committed
    /// gap in which a concurrent append can acknowledge an undelivered turn.
    pub async fn replace_active_execution_with_queued(
        &self,
        execution_id: &str,
        idempotency_key: &str,
        successor_metadata: Value,
        terminal: OwnedTerminalEvent,
    ) -> Result<Option<(SessionExecution, SessionExecution)>, SessionStoreError> {
        let (terminal_status, _session_status, error, event_type, payload) = terminal.into_parts();
        let successor_id = prefixed_id("exe");
        let mut tx = self.pool.begin().await?;
        if !lock_execution_input_delivery_lifecycle(&mut tx, execution_id).await? {
            tx.commit().await?;
            return Ok(None);
        }
        let old = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            update session_executions
            set status = $2, error = $3, completed_at = coalesce(completed_at, now()),
                stdout_owner_id = null, stdout_owner_lease_expires_at = null, updated_at = now()
            where execution_id = $1 and status in ($4, $5)
              and not exists (
                    select 1 from session_input_deliveries delivery
                    where delivery.execution_id = session_executions.execution_id
                      and delivery.state in ('pending', 'claimed', 'ambiguous')
              )
            returning execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            "#,
        )
        .bind(execution_id)
        .bind(terminal_status.as_ref())
        .bind(&error)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .fetch_optional(&mut *tx)
        .await?;
        let Some(old) = old else {
            tx.commit().await?;
            return Ok(None);
        };
        let thread_key = old.thread_key.clone();
        let successor = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            insert into session_executions (execution_id, thread_key, idempotency_key, status, metadata)
            values ($1, $2, $3, $4, $5)
            returning execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            "#,
        )
        .bind(&successor_id)
        .bind(&thread_key)
        .bind(idempotency_key)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(successor_metadata)
        .fetch_one(&mut *tx)
        .await?;
        sqlx::query(
            r#"
            update sessions set status = $2, updated_at = now() where thread_key = $1
            "#,
        )
        .bind(&thread_key)
        .bind(SessionStatus::Executing.as_ref())
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            r#"
            insert into session_events (thread_key, execution_id, event_type, payload)
            values ($1, $2, $3, $4)
            "#,
        )
        .bind(&thread_key)
        .bind(execution_id)
        .bind(event_type)
        .bind(payload)
        .execute(&mut *tx)
        .await?;
        let old = old.try_into()?;
        let successor = successor.try_into()?;
        tx.commit().await?;
        Ok(Some((old, successor)))
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
                s.sandbox_resource_uid as resource_uid,
                s.sandbox_assignment_epoch as assignment_epoch,
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
                s.sandbox_resource_uid as resource_uid,
                s.sandbox_assignment_epoch as assignment_epoch,
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
            select thread_key,
                   sandbox_id as sandbox_id,
                   sandbox_resource_uid as resource_uid,
                   sandbox_assignment_epoch as assignment_epoch
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
                sandbox_metadata_trace_assignment_epoch = null,
                sandbox_resource_uid = null,
                sandbox_assignment_epoch = null,
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
        resource_uid: Option<&str>,
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
                sandbox_metadata_trace_resource_uid = null,
                sandbox_resource_uid = $13,
                sandbox_assignment_epoch = md5(random()::text || clock_timestamp()::text),
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
        .bind(resource_uid)
        .fetch_one(&self.pool)
        .await?;

        row.try_into()
    }

    pub async fn update_sandbox_assignment_if_matches(
        &self,
        thread_key: &ThreadKey,
        sandbox_id: &str,
        resource_uid: Option<&str>,
        capabilities: &SandboxCapabilities,
        expected: &SandboxAssignmentSnapshot,
    ) -> Result<bool, SessionStoreError> {
        let result = sqlx::query(
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
                sandbox_metadata_trace_assignment_epoch = null,
                sandbox_metadata_trace_resource_uid = null,
                sandbox_resource_uid = $13,
                sandbox_assignment_epoch = md5(random()::text || clock_timestamp()::text),
                sandbox_last_active_at = now(),
                updated_at = now()
            where thread_key = $1
              and sandbox_id is not distinct from $14
              and sandbox_resource_uid is not distinct from $15
              and sandbox_assignment_epoch is not distinct from $16
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
        .bind(resource_uid)
        .bind(expected.sandbox_id.as_deref())
        .bind(expected.resource_uid.as_deref())
        .bind(expected.assignment_epoch.as_deref())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_sandbox_assignment_if_metadata_trace_config_active(
        &self,
        thread_key: &ThreadKey,
        sandbox_id: &str,
        capabilities: &SandboxCapabilities,
        identity: &MetadataTraceConfigIdentity,
        expected: &SandboxAssignmentSnapshot,
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
                sandbox_resource_uid = $15,
                sandbox_assignment_epoch = md5(random()::text || clock_timestamp()::text),
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
              and sandbox_resource_uid is not distinct from $19
              and sandbox_assignment_epoch is not distinct from $20
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
        .bind(expected.sandbox_id.as_deref())
        .bind(expected.resource_uid.as_deref())
        .bind(expected.assignment_epoch.as_deref())
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
                sandbox_resource_uid = null,
                sandbox_assignment_epoch = null,
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
                sandbox_resource_uid = null,
                sandbox_assignment_epoch = null,
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

    /// Clear a pending drain only if the exact disabled revision that claimed
    /// it is still current. A stale reconciler must not mutate a later grant.
    pub async fn complete_metadata_trace_drain_if_current_and_empty(
        &self,
        source: &str,
        workspace_id: &str,
        user_id: &str,
        disabled_revision: i64,
    ) -> Result<bool, SessionStoreError> {
        let result = sqlx::query(
            r#"
            update metadata_trace_consents consent
            set drain_pending = false, updated_at = now()
            where source = $1 and workspace_id = $2 and user_id = $3
              and revision = $7 and enabled is false and drain_pending is true
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
            "#,
        )
        .bind(source)
        .bind(workspace_id)
        .bind(user_id)
        .bind(source)
        .bind(workspace_id)
        .bind(user_id)
        .bind(disabled_revision)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
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
                   sandbox_resource_uid,
                   sandbox_assignment_epoch,
                   sandbox_metadata_trace_assignment_epoch
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
            sandbox_resource_uid: current_assignment.sandbox_resource_uid,
            sandbox_assignment_epoch: current_assignment.sandbox_assignment_epoch,
            sandbox_metadata_trace_assignment_epoch: current_assignment
                .sandbox_metadata_trace_assignment_epoch,
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
                sandbox_metadata_trace_assignment_epoch = null,
                sandbox_resource_uid = null,
                sandbox_assignment_epoch = null,
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
        resource_uid: Option<&str>,
        workload_key: &str,
    ) -> Result<(), SessionStoreError> {
        sqlx::query(
            r#"
            insert into session_warm_sandboxes
                (sandbox_id, sandbox_resource_uid, sandbox_assignment_epoch, workload_key, status)
            values ($1, $2, md5(random()::text || clock_timestamp()::text), $3, 'ready')
            "#,
        )
        .bind(sandbox_id)
        .bind(resource_uid)
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

    pub async fn list_ready_warm_sandboxes(
        &self,
    ) -> Result<Vec<ReadyWarmSandbox>, SessionStoreError> {
        let sandboxes = sqlx::query_as::<_, ReadyWarmSandbox>(
            r#"
            select sandbox_id,
                   sandbox_resource_uid as resource_uid,
                   sandbox_assignment_epoch as assignment_epoch
            from session_warm_sandboxes
            where status = 'ready'
            order by created_at, sandbox_id
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(sandboxes)
    }

    pub async fn claim_ready_warm_sandbox(
        &self,
        workload_key: &str,
        thread_key: &str,
    ) -> Result<Option<ReadyWarmSandbox>, SessionStoreError> {
        let sandbox = sqlx::query_as::<_, ReadyWarmSandbox>(
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
            returning warm.sandbox_id,
                      warm.sandbox_resource_uid as resource_uid,
                      warm.sandbox_assignment_epoch as assignment_epoch
            "#,
        )
        .bind(workload_key)
        .bind(thread_key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(sandbox)
    }

    pub async fn reserve_ready_warm_sandboxes_for_eviction(
        &self,
        limit: i64,
    ) -> Result<Vec<ReadyWarmSandbox>, SessionStoreError> {
        let rows = sqlx::query_as::<_, ReadyWarmSandbox>(
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
            returning warm.sandbox_id,
                      warm.sandbox_resource_uid as resource_uid,
                      warm.sandbox_assignment_epoch as assignment_epoch
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
    ) -> Result<Vec<ReadyWarmSandbox>, SessionStoreError> {
        let rows = sqlx::query_as::<_, ReadyWarmSandbox>(
            r#"
            select sandbox_id,
                   sandbox_resource_uid as resource_uid,
                   sandbox_assignment_epoch as assignment_epoch
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

    /// Retire a warm-pool row only if its immutable backend identity still
    /// matches the reservation that performed the cleanup.
    pub async fn mark_warm_sandbox_failed_if_matches(
        &self,
        sandbox: &ReadyWarmSandbox,
        error: &str,
    ) -> Result<bool, SessionStoreError> {
        let result = sqlx::query(
            r#"
            update session_warm_sandboxes
            set status = 'failed', last_error = $2, updated_at = now()
            where sandbox_id = $1
              and sandbox_resource_uid is not distinct from $3
              and sandbox_assignment_epoch is not distinct from $4
            "#,
        )
        .bind(&sandbox.sandbox_id)
        .bind(error)
        .bind(&sandbox.resource_uid)
        .bind(&sandbox.assignment_epoch)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Retire a claimed warm sandbox only while its claimant still owns the
    /// exact durable session assignment. Warm-pool and session epochs are
    /// separate fences, so the session's UID+epoch identifies the claimant's
    /// assignment while the warm row is fenced by its stable resource UID.
    pub async fn mark_claimed_warm_sandbox_failed_for_assignment(
        &self,
        thread_key: &ThreadKey,
        sandbox_id: &str,
        resource_uid: &str,
        assignment_epoch: &str,
        error: &str,
    ) -> Result<bool, SessionStoreError> {
        let result = sqlx::query(
            r#"
            update session_warm_sandboxes warm
            set status = 'failed', last_error = $5, updated_at = now()
            where warm.sandbox_id = $1
              and warm.status = 'claimed'
              and warm.claimed_thread_key = $2
              and warm.sandbox_resource_uid = $3
              and exists (
                    select 1
                    from sessions
                    where thread_key = $2
                      and sandbox_id = $1
                      and sandbox_resource_uid = $3
                      and sandbox_assignment_epoch = $4
              )
            "#,
        )
        .bind(sandbox_id)
        .bind(thread_key.as_str())
        .bind(resource_uid)
        .bind(assignment_epoch)
        .bind(error)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
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
            returning session.thread_key, session.title, session.sandbox_id, session.sandbox_repo_cache_enabled, session.sandbox_repo_cache_access, session.sandbox_observability_enabled, session.sandbox_api_server_enabled, session.sandbox_metadata_trace_enabled, session.sandbox_metadata_trace_expires_at, session.sandbox_metadata_trace_subject_hash, session.sandbox_metadata_trace_consent_revision, session.sandbox_metadata_trace_config_fingerprint, session.sandbox_metadata_trace_config_generation, session.harness_type, session.harness_thread_id, session.persona_id, session.status, session.iron_control_principal, session.proxy_labels, session.sandbox_last_active_at, session.created_at, session.updated_at
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
    #[error("input delivery idempotency key was already used for a different payload")]
    InputDeliveryIdempotencyConflict,
    #[error(
        "existing execution {execution_id} has no durable initial input delivery; it must be drained before the input-delivery cutover"
    )]
    InputDeliveryMissingForExistingExecution { execution_id: String },
    #[error("idempotency key was already used for a different metadata trace consent request")]
    MetadataTraceIdempotencyConflict,
    #[error("metadata trace consent request is incomplete and cannot be safely replayed")]
    MetadataTraceIdempotencyIncomplete,
    #[error("metadata trace consent request was consumed by a later consent boundary")]
    MetadataTraceIdempotencyReplayFenced,
    #[error("metadata trace consent changed after confirmation preview")]
    MetadataTraceConsentRevisionChanged,
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

#[derive(Debug, FromRow)]
struct SandboxAssignmentIdentityRow {
    assignment_epoch: String,
    resource_uid: Option<String>,
}

#[derive(Debug, FromRow)]
struct SandboxAssignmentSnapshotRow {
    sandbox_id: Option<String>,
    resource_uid: Option<String>,
    assignment_epoch: Option<String>,
}

impl From<SandboxAssignmentIdentityRow> for SandboxAssignmentIdentity {
    fn from(row: SandboxAssignmentIdentityRow) -> Self {
        Self {
            assignment_epoch: row.assignment_epoch,
            resource_uid: row.resource_uid,
        }
    }
}

impl From<SandboxAssignmentSnapshotRow> for SandboxAssignmentSnapshot {
    fn from(row: SandboxAssignmentSnapshotRow) -> Self {
        Self {
            sandbox_id: row.sandbox_id,
            resource_uid: row.resource_uid,
            assignment_epoch: row.assignment_epoch,
        }
    }
}

#[derive(Debug, FromRow)]
struct InputDeliveryCandidateRow {
    delivery_id: String,
    execution_id: String,
}

#[derive(Debug, FromRow)]
struct SessionInputDeliveryRow {
    delivery_id: String,
    thread_key: String,
    execution_id: String,
    sequence: i64,
    idempotency_key: String,
    message_ids: Value,
    input_lines: Value,
    input_sha256: String,
    input_line_count: i32,
    boundary_fingerprint: String,
    state: String,
    owner_id: Option<String>,
    owner_generation: i64,
    owner_lease_expires_at: Option<OffsetDateTime>,
    sandbox_id: Option<String>,
    sandbox_resource_uid: Option<String>,
    sandbox_assignment_epoch: Option<String>,
    attempts: i32,
    last_error: Option<String>,
    created_at: OffsetDateTime,
    claimed_at: Option<OffsetDateTime>,
    flushed_at: Option<OffsetDateTime>,
    failed_at: Option<OffsetDateTime>,
    updated_at: OffsetDateTime,
}

#[derive(Debug, FromRow)]
struct PersistedPreparedMessageRow {
    message_id: String,
    role: String,
    parts: Value,
    metadata: Value,
}

impl TryFrom<SessionInputDeliveryRow> for SessionInputDelivery {
    type Error = SessionStoreError;

    fn try_from(row: SessionInputDeliveryRow) -> Result<Self, Self::Error> {
        Ok(Self {
            delivery_id: row.delivery_id,
            thread_key: parse_persisted(row.thread_key)?,
            execution_id: row.execution_id,
            sequence: row.sequence,
            idempotency_key: row.idempotency_key,
            message_ids: serde_json::from_value(row.message_ids)
                .map_err(|error| SessionStoreError::InvalidPersistedValue(error.to_string()))?,
            input_lines: serde_json::from_value(row.input_lines)
                .map_err(|error| SessionStoreError::InvalidPersistedValue(error.to_string()))?,
            input_sha256: row.input_sha256,
            input_line_count: row.input_line_count,
            boundary_fingerprint: row.boundary_fingerprint,
            state: parse_persisted(row.state)?,
            owner_id: row.owner_id,
            owner_generation: row.owner_generation,
            owner_lease_expires_at: row.owner_lease_expires_at,
            sandbox_id: row.sandbox_id,
            sandbox_resource_uid: row.sandbox_resource_uid,
            sandbox_assignment_epoch: row.sandbox_assignment_epoch,
            attempts: row.attempts,
            last_error: row.last_error,
            created_at: row.created_at,
            claimed_at: row.claimed_at,
            flushed_at: row.flushed_at,
            failed_at: row.failed_at,
            updated_at: row.updated_at,
        })
    }
}

#[derive(Debug, FromRow)]
struct FlushSessionRow {
    thread_key: String,
    sandbox_id: Option<String>,
    sandbox_resource_uid: Option<String>,
    sandbox_assignment_epoch: Option<String>,
    sandbox_metadata_trace_enabled: Option<bool>,
    sandbox_metadata_trace_expires_at: Option<OffsetDateTime>,
    sandbox_metadata_trace_subject_hash: Option<String>,
    sandbox_metadata_trace_consent_revision: Option<i64>,
    sandbox_metadata_trace_config_fingerprint: Option<String>,
    sandbox_metadata_trace_config_generation: Option<i64>,
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
    resource_uid: Option<String>,
    assignment_epoch: Option<String>,
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
        resource_uid: row.resource_uid,
        assignment_epoch: row.assignment_epoch,
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
    resource_uid: Option<String>,
    assignment_epoch: Option<String>,
    latest_execution_id: Option<String>,
    last_active_at: OffsetDateTime,
}

impl TryFrom<SandboxCapacityCandidateRow> for SandboxCapacityCandidate {
    type Error = SessionStoreError;

    fn try_from(row: SandboxCapacityCandidateRow) -> Result<Self, Self::Error> {
        Ok(Self {
            thread_key: parse_persisted(row.thread_key)?,
            sandbox_id: row.sandbox_id,
            resource_uid: row.resource_uid,
            assignment_epoch: row.assignment_epoch,
            latest_execution_id: row.latest_execution_id,
            last_active_at: row.last_active_at,
        })
    }
}

#[derive(Debug, FromRow)]
struct WorkflowOwnedSandboxRow {
    thread_key: String,
    sandbox_id: String,
    resource_uid: Option<String>,
    assignment_epoch: Option<String>,
}

impl TryFrom<WorkflowOwnedSandboxRow> for WorkflowOwnedSandbox {
    type Error = SessionStoreError;

    fn try_from(row: WorkflowOwnedSandboxRow) -> Result<Self, Self::Error> {
        Ok(Self {
            thread_key: parse_persisted(row.thread_key)?,
            sandbox_id: row.sandbox_id,
            resource_uid: row.resource_uid,
            assignment_epoch: row.assignment_epoch,
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
        ExecutionStatus, HarnessType, MessageRole, SandboxCapabilities, Session,
        SessionMessageInput, SessionStatus, ThreadKey,
    };
    use serde_json::{Value, json};
    use time::{Duration as TimeDuration, OffsetDateTime};
    use uuid::Uuid;

    static TRACE_CONFIG_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    use super::{
        AppendMessagesWithoutActiveExecution, IdleSandboxCandidateRow, MetadataTraceConfigIdentity,
        OwnedTerminalEvent, PgSessionStore, PreparedInputDelivery, PreparedSessionMessage,
        ReadyWarmSandbox, SandboxAssignmentIdentity, SandboxAssignmentSnapshot,
        SessionEventNotification, SessionRow, SessionStoreError, input_lines_sha256,
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

    fn prepared_delivery(key: &str, message_id: &str) -> super::PreparedInputDelivery {
        super::PreparedInputDelivery {
            idempotency_key: key.to_owned(),
            message_ids: vec![message_id.to_owned()],
            input_lines: vec![format!("{{\"message_id\":\"{message_id}\"}}")],
            boundary_fingerprint: "boundary-test".to_owned(),
        }
    }

    #[tokio::test]
    async fn initial_input_delivery_idempotency_requires_exact_payload() {
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:input-idempotency-{}", Uuid::new_v4())).unwrap();
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
        let prepared = prepared_delivery("delivery-key", "msg-original");
        let first = store
            .create_execution_with_initial_input_delivery(
                &thread_key,
                "execution-key",
                json!({}),
                &prepared,
            )
            .await
            .unwrap();
        let replay = store
            .create_execution_with_initial_input_delivery(
                &thread_key,
                "execution-key",
                json!({"ignored": true}),
                &prepared,
            )
            .await
            .unwrap();
        assert!(first.created);
        assert!(!replay.created);
        assert_eq!(first.delivery.delivery_id, replay.delivery.delivery_id);
        let conflict = store
            .create_execution_with_initial_input_delivery(
                &thread_key,
                "execution-key",
                json!({}),
                &prepared_delivery("delivery-key", "msg-different"),
            )
            .await;
        assert!(matches!(
            conflict,
            Err(SessionStoreError::InputDeliveryIdempotencyConflict)
        ));
    }

    #[tokio::test]
    async fn existing_execution_without_delivery_cannot_accept_a_new_retry_payload() {
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:legacy-execution-{}", Uuid::new_v4())).unwrap();
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
        let legacy = store
            .create_execution(&thread_key, Some("legacy-execution-key"), json!({}))
            .await
            .unwrap();
        let result = store
            .create_execution_with_initial_input_delivery(
                &thread_key,
                "legacy-execution-key",
                json!({}),
                &prepared_delivery("execute:legacy-execution-key", "msg-new"),
            )
            .await;
        assert!(matches!(
            result,
            Err(SessionStoreError::InputDeliveryMissingForExistingExecution { execution_id })
                if execution_id == legacy.execution.execution_id
        ));
        assert!(
            store
                .list_unresolved_input_deliveries()
                .await
                .unwrap()
                .into_iter()
                .all(|delivery| delivery.execution_id != legacy.execution.execution_id),
            "the retry must not fabricate an input obligation for a legacy execution"
        );
        assert!(
            store
                .complete_execution_if_active(&legacy.execution.execution_id)
                .await
                .unwrap()
                .is_some()
        );
        let terminal_replay = store
            .create_execution_with_initial_input_delivery(
                &thread_key,
                "legacy-execution-key",
                json!({}),
                &prepared_delivery("execute:legacy-execution-key", "msg-terminal-retry"),
            )
            .await;
        assert!(matches!(
            terminal_replay,
            Err(SessionStoreError::InputDeliveryMissingForExistingExecution { execution_id })
                if execution_id == legacy.execution.execution_id
        ));
    }

    #[tokio::test]
    async fn no_active_append_decision_is_serializable_with_execution_creation() {
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:append-decision-{}", Uuid::new_v4())).unwrap();
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
        let message_id = format!("msg-no-active-{}", Uuid::new_v4().simple());
        let message = PreparedSessionMessage {
            message_id: message_id.clone(),
            input: SessionMessageInput {
                client_message_id: Some("no-active".to_owned()),
                role: MessageRole::User,
                parts: vec![json!({"type": "text", "text": "first"})],
                metadata: json!({}),
            },
        };
        let outcome = store
            .append_prepared_messages_if_no_active_execution(
                &thread_key,
                std::slice::from_ref(&message),
            )
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            AppendMessagesWithoutActiveExecution::Appended(message_ids)
                if message_ids == vec![message_id]
        ));
        let active = store
            .create_execution_with_initial_input_delivery(
                &thread_key,
                "active-execution-key",
                json!({}),
                &prepared_delivery("execute:active-execution-key", "msg-execution"),
            )
            .await
            .unwrap();
        let outcome = store
            .append_prepared_messages_if_no_active_execution(
                &thread_key,
                std::slice::from_ref(&message),
            )
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            AppendMessagesWithoutActiveExecution::Active(execution)
                if execution.execution_id == active.execution.execution_id
        ));
    }

    #[tokio::test]
    async fn unresolved_delivery_blocks_terminal_transition() {
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:input-barrier-{}", Uuid::new_v4())).unwrap();
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
        let prepared = prepared_delivery("delivery-key", "msg-1");
        let created = store
            .create_execution_with_initial_input_delivery(
                &thread_key,
                "execution-key",
                json!({}),
                &prepared,
            )
            .await
            .unwrap();
        assert!(
            store
                .complete_execution_if_active(&created.execution.execution_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .fail_execution_if_active(
                    &created.execution.execution_id,
                    "must not bypass delivery"
                )
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn terminal_failed_delivery_scrubs_plaintext_and_retains_audit_digest() {
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:input-failed-scrub-{}", Uuid::new_v4())).unwrap();
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
        let prepared = PreparedInputDelivery {
            idempotency_key: "delivery-failed-scrub".to_owned(),
            message_ids: vec!["msg-failed-scrub".to_owned()],
            input_lines: vec!["sensitive exact input".to_owned()],
            boundary_fingerprint: "boundary-test".to_owned(),
        };
        let created = store
            .create_execution_with_initial_input_delivery(
                &thread_key,
                "execution-failed-scrub",
                json!({}),
                &prepared,
            )
            .await
            .unwrap();
        let claim = store
            .claim_next_input_delivery(
                "owner-failed-scrub",
                Duration::from_secs(60),
                Some(&created.execution.execution_id),
                None,
            )
            .await
            .unwrap()
            .unwrap();
        assert!(
            store
                .mark_input_delivery_failed(
                    &claim.delivery.delivery_id,
                    "owner-failed-scrub",
                    claim.delivery.owner_generation,
                    "input rejected before sandbox write",
                )
                .await
                .unwrap()
        );
        let (state, persisted_input, persisted_digest, persisted_count) =
            sqlx::query_as::<_, (String, Value, String, i32)>(
                "select state, input_lines, input_sha256, input_line_count from session_input_deliveries where delivery_id = $1",
            )
            .bind(&claim.delivery.delivery_id)
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(state, "failed");
        assert_eq!(persisted_input, json!([]));
        assert_eq!(persisted_digest, input_lines_sha256(&prepared.input_lines));
        assert_eq!(persisted_count, 1);
        assert!(
            store
                .fail_execution_if_active(&created.execution.execution_id, "input rejected")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn stale_delivery_generation_cannot_begin_flush() {
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:input-generation-{}", Uuid::new_v4())).unwrap();
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
        let capabilities = SandboxCapabilities::default_enabled();
        store
            .update_sandbox_assignment(&thread_key, "sbx-input", Some("uid-input"), &capabilities)
            .await
            .unwrap();
        let identity = store
            .current_sandbox_assignment_identity(&thread_key, "sbx-input")
            .await
            .unwrap()
            .unwrap();
        let created = store
            .create_execution_with_initial_input_delivery(
                &thread_key,
                "execution-key",
                json!({}),
                &prepared_delivery("delivery-key", "msg-1"),
            )
            .await
            .unwrap();
        let first = store
            .claim_next_input_delivery(
                "owner-a",
                Duration::from_secs(60),
                Some(&created.execution.execution_id),
                None,
            )
            .await
            .unwrap()
            .unwrap();
        sqlx::query(
            "update session_input_deliveries set owner_lease_expires_at = clock_timestamp() - interval '1 second' where delivery_id = $1",
        )
        .bind(&first.delivery.delivery_id)
        .execute(store.pool())
        .await
        .unwrap();
        sqlx::query(
            "update session_executions set stdout_owner_lease_expires_at = clock_timestamp() - interval '1 second' where execution_id = $1",
        )
        .bind(&created.execution.execution_id)
        .execute(store.pool())
        .await
        .unwrap();
        assert!(
            !store
                .mark_input_delivery_failed(
                    &first.delivery.delivery_id,
                    "owner-a",
                    first.delivery.owner_generation,
                    "expired owner must not resolve input",
                )
                .await
                .unwrap()
        );
        assert!(
            !store
                .mark_input_delivery_ambiguous(
                    &first.delivery.delivery_id,
                    "owner-a",
                    first.delivery.owner_generation,
                    "expired owner must not rewrite input",
                )
                .await
                .unwrap()
        );
        let second = store
            .claim_next_input_delivery(
                "owner-b",
                Duration::from_secs(60),
                Some(&created.execution.execution_id),
                None,
            )
            .await
            .unwrap()
            .unwrap();
        assert!(second.delivery.owner_generation > first.delivery.owner_generation);
        assert!(
            store
                .rebind_claimed_input_delivery_boundary(
                    &second.delivery.delivery_id,
                    "owner-b",
                    second.delivery.owner_generation,
                    "boundary-current",
                    json!({"metadata_trace_enabled": false}),
                )
                .await
                .unwrap()
        );
        assert!(
            store
                .begin_input_delivery_flush(
                    &first.delivery.delivery_id,
                    "owner-a",
                    first.delivery.owner_generation,
                    "sbx-input",
                    &capabilities,
                    "boundary-test",
                    &identity,
                )
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn terminal_race_never_persists_messages_without_a_delivery() {
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:append-terminal-race-{}", Uuid::new_v4())).unwrap();
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
        let execution = store
            .create_execution(&thread_key, Some("terminal-race"), json!({}))
            .await
            .unwrap()
            .execution;
        store
            .fail_execution_if_active(&execution.execution_id, "terminal won")
            .await
            .unwrap();
        let input = SessionMessageInput {
            role: MessageRole::User,
            parts: vec![json!({"type": "text", "text": "must retry"})],
            metadata: json!({}),
            client_message_id: Some("terminal-race-message".to_owned()),
        };
        let message = PreparedSessionMessage {
            message_id: "msg-terminal-race".to_owned(),
            input,
        };
        let prepared = PreparedInputDelivery {
            idempotency_key: "delivery-terminal-race".to_owned(),
            message_ids: vec![message.message_id.clone()],
            input_lines: vec!["must retry".to_owned()],
            boundary_fingerprint: "boundary-terminal-race".to_owned(),
        };
        assert!(
            store
                .append_messages_and_enqueue_input_delivery(
                    &thread_key,
                    &execution.execution_id,
                    &[message],
                    &prepared,
                )
                .await
                .unwrap()
                .is_none()
        );
        assert!(store.list_messages(&thread_key).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn generic_sandbox_assignment_updates_are_cas_fenced() {
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:generic-assignment-cas-{}", Uuid::new_v4())).unwrap();
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
        let capabilities = SandboxCapabilities::default_enabled();
        store
            .update_sandbox_assignment(&thread_key, "sbx-old", Some("uid-old"), &capabilities)
            .await
            .unwrap();
        let stale_snapshot = store
            .sandbox_assignment_snapshot(&thread_key)
            .await
            .unwrap();
        store
            .update_sandbox_assignment(
                &thread_key,
                "sbx-old",
                Some("uid-replacement"),
                &capabilities,
            )
            .await
            .unwrap();
        assert!(
            !store
                .update_sandbox_assignment_if_matches(
                    &thread_key,
                    "sbx-stale",
                    Some("uid-stale"),
                    &capabilities,
                    &stale_snapshot,
                )
                .await
                .unwrap()
        );
        let current_snapshot = store
            .sandbox_assignment_snapshot(&thread_key)
            .await
            .unwrap();
        assert!(
            store
                .update_sandbox_assignment_if_matches(
                    &thread_key,
                    "sbx-new",
                    Some("uid-new"),
                    &capabilities,
                    &current_snapshot,
                )
                .await
                .unwrap()
        );
        let assignment = store
            .lock_sandbox_assignment_for_reconciliation(&thread_key, "sbx-new")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(assignment.resource_uid(), Some("uid-new"));
        assert!(assignment.assignment_epoch().is_some());
        assignment.rollback().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn input_flush_commit_serializes_before_terminal_output() {
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key = ThreadKey::parse(format!("test:input-flush-{}", Uuid::new_v4())).unwrap();
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
        let capabilities = SandboxCapabilities::default_enabled();
        store
            .update_sandbox_assignment(&thread_key, "sbx-flush", Some("uid-flush"), &capabilities)
            .await
            .unwrap();
        let assignment = store
            .current_sandbox_assignment_identity(&thread_key, "sbx-flush")
            .await
            .unwrap()
            .unwrap();
        let prepared = prepared_delivery("delivery-key", "msg-1");
        let created = store
            .create_execution_with_initial_input_delivery(
                &thread_key,
                "execution-key",
                json!({}),
                &prepared,
            )
            .await
            .unwrap();
        let claim = store
            .claim_next_input_delivery(
                "owner-flush",
                Duration::from_secs(60),
                Some(&created.execution.execution_id),
                None,
            )
            .await
            .unwrap()
            .unwrap();

        let wrong_assignment = SandboxAssignmentIdentity {
            assignment_epoch: assignment.assignment_epoch.clone(),
            resource_uid: Some("different-uid".to_owned()),
        };
        assert!(
            store
                .begin_input_delivery_flush(
                    &claim.delivery.delivery_id,
                    "owner-flush",
                    claim.delivery.owner_generation,
                    "sbx-flush",
                    &capabilities,
                    "boundary-test",
                    &wrong_assignment,
                )
                .await
                .unwrap()
                .is_none(),
            "a reused sandbox name with a different backend UID must not receive input"
        );

        let guard = store
            .begin_input_delivery_flush(
                &claim.delivery.delivery_id,
                "owner-flush",
                claim.delivery.owner_generation,
                "sbx-flush",
                &capabilities,
                "boundary-test",
                &assignment,
            )
            .await
            .unwrap()
            .unwrap();
        let terminal_store = store.clone();
        let execution_id = created.execution.execution_id.clone();
        let terminal = tokio::spawn(async move {
            terminal_store
                .terminalize_execution_and_append_event_if_stdout_owner(
                    &execution_id,
                    "owner-flush",
                    OwnedTerminalEvent::Completed { payload: json!({}) },
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !terminal.is_finished(),
            "terminal output must wait behind the delivery transaction"
        );
        let event = guard.commit().await.unwrap().unwrap();
        assert_eq!(event.event_type, "session.input_flushed");
        assert_eq!(
            event.payload,
            json!({
                "delivery_id": claim.delivery.delivery_id,
                "input_sha256": input_lines_sha256(&prepared.input_lines),
                "input_line_count": 1,
            })
        );
        let (persisted_input, persisted_digest, persisted_count) =
            sqlx::query_as::<_, (Value, String, i32)>(
                "select input_lines, input_sha256, input_line_count from session_input_deliveries where delivery_id = $1",
            )
            .bind(&claim.delivery.delivery_id)
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(persisted_input, json!([]));
        assert_eq!(persisted_digest, input_lines_sha256(&prepared.input_lines));
        assert_eq!(persisted_count, 1);
        assert!(terminal.await.unwrap().unwrap().is_some());
        let replay = store
            .create_execution_with_initial_input_delivery(
                &thread_key,
                "execution-key",
                json!({}),
                &prepared,
            )
            .await
            .unwrap();
        assert!(!replay.created);
        assert_eq!(replay.delivery.delivery_id, claim.delivery.delivery_id);
        assert!(replay.delivery.input_lines.is_empty());
    }

    #[tokio::test]
    async fn ambiguous_flush_replays_the_exact_persisted_payload() {
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key = ThreadKey::parse(format!("test:input-replay-{}", Uuid::new_v4())).unwrap();
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
        let capabilities = SandboxCapabilities::default_enabled();
        store
            .update_sandbox_assignment(&thread_key, "sbx-replay", Some("uid-replay"), &capabilities)
            .await
            .unwrap();
        let assignment = store
            .current_sandbox_assignment_identity(&thread_key, "sbx-replay")
            .await
            .unwrap()
            .unwrap();
        let created = store
            .create_execution_with_initial_input_delivery(
                &thread_key,
                "execution-key",
                json!({}),
                &prepared_delivery("delivery-key", "msg-1"),
            )
            .await
            .unwrap();
        let first = store
            .claim_next_input_delivery(
                "owner-a",
                Duration::from_secs(60),
                Some(&created.execution.execution_id),
                None,
            )
            .await
            .unwrap()
            .unwrap();
        let guard = store
            .begin_input_delivery_flush(
                &first.delivery.delivery_id,
                "owner-a",
                first.delivery.owner_generation,
                "sbx-replay",
                &capabilities,
                "boundary-test",
                &assignment,
            )
            .await
            .unwrap()
            .unwrap();
        guard.rollback().await.unwrap();
        sqlx::query(
            "update session_input_deliveries set owner_lease_expires_at = clock_timestamp() - interval '1 second' where delivery_id = $1",
        )
        .bind(&first.delivery.delivery_id)
        .execute(store.pool())
        .await
        .unwrap();
        sqlx::query(
            "update session_executions set stdout_owner_lease_expires_at = clock_timestamp() - interval '1 second' where execution_id = $1",
        )
        .bind(&created.execution.execution_id)
        .execute(store.pool())
        .await
        .unwrap();
        let replay = store
            .claim_next_input_delivery(
                "owner-b",
                Duration::from_secs(60),
                Some(&created.execution.execution_id),
                None,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replay.delivery.delivery_id, first.delivery.delivery_id);
        assert_eq!(replay.delivery.input_lines, first.delivery.input_lines);
        assert_eq!(replay.delivery.message_ids, first.delivery.message_ids);
        assert!(replay.delivery.owner_generation > first.delivery.owner_generation);
        assert_eq!(replay.delivery.attempts, 2);
    }

    #[tokio::test]
    async fn replacement_delivery_survives_driver_loss_and_blocks_a_third_actor() {
        let Some(store) = test_store().await else {
            return;
        };
        let (_thread_key, old_execution_id) =
            running_execution_with_stdout_owner(&store, "replacement-ledger", "owner-a").await;
        let mut successor_input = prepared_delivery("replacement-u2", "unused-message");
        successor_input.message_ids.clear();
        successor_input.input_lines = vec!["exact-u2-input".to_owned()];
        let replacement = store
            .replace_active_execution_with_initial_input_delivery(
                &old_execution_id,
                &[],
                json!({"actor": "u2"}),
                OwnedTerminalEvent::Failed {
                    error: "actor boundary changed".to_owned(),
                    payload: json!({}),
                },
                &successor_input,
            )
            .await
            .unwrap()
            .unwrap();
        let successor = replacement.1;
        let delivery = replacement.2;
        assert_eq!(delivery.input_lines, vec!["exact-u2-input"]);

        let mut third_actor_input = prepared_delivery("replacement-u3", "unused-message");
        third_actor_input.message_ids.clear();
        third_actor_input.input_lines = vec!["u3-must-wait".to_owned()];
        assert!(
            store
                .replace_active_execution_with_initial_input_delivery(
                    &successor.execution_id,
                    &[],
                    json!({"actor": "u3"}),
                    OwnedTerminalEvent::Failed {
                        error: "actor boundary changed again".to_owned(),
                        payload: json!({}),
                    },
                    &third_actor_input,
                )
                .await
                .unwrap()
                .is_none(),
            "a third actor cannot preempt an unresolved successor delivery"
        );

        let recovered = store
            .claim_next_input_delivery(
                "owner-b",
                Duration::from_secs(60),
                Some(&successor.execution_id),
                Some(&delivery.delivery_id),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recovered.delivery.input_lines, vec!["exact-u2-input"]);
        assert_eq!(recovered.delivery.owner_id.as_deref(), Some("owner-b"));
        assert!(
            store
                .mark_input_delivery_failed(
                    &recovered.delivery.delivery_id,
                    "owner-b",
                    recovered.delivery.owner_generation,
                    "test cleanup",
                )
                .await
                .unwrap()
        );
        assert!(
            store
                .terminalize_execution_and_append_event_if_stdout_owner(
                    &successor.execution_id,
                    "owner-b",
                    OwnedTerminalEvent::Failed {
                        error: "test cleanup".to_owned(),
                        payload: json!({}),
                    },
                )
                .await
                .unwrap()
                .is_some()
        );
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

    fn postgres_timestamp(value: OffsetDateTime) -> OffsetDateTime {
        value
            .replace_nanosecond(value.nanosecond() / 1_000 * 1_000)
            .expect("microsecond precision is a valid nanosecond value")
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
    async fn persisted_trace_state_detects_an_activated_generation() {
        let Some(store) = test_store().await else {
            return;
        };
        let _trace_config_lock = TRACE_CONFIG_TEST_LOCK.lock().await;
        let identity = MetadataTraceConfigIdentity {
            generation: next_trace_generation(&store).await,
            fingerprint: format!("startup-fence-{}", Uuid::new_v4()),
            enabled: false,
        };
        store
            .activate_metadata_trace_config(&identity)
            .await
            .unwrap();

        assert!(store.has_persisted_metadata_trace_state().await.unwrap());
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
        let expiry = postgres_timestamp(OffsetDateTime::now_utc() + TimeDuration::hours(1));
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
            .update_sandbox_assignment(&thread_key, "sbx-trace-expiry", None, &capabilities)
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
                Some(0),
                key,
                request_hash,
            ),
            second_store.grant_metadata_trace_consent_idempotent(
                "slack",
                &workspace_id,
                &user_id,
                expires_at,
                Some(0),
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
                Some(0),
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

    #[tokio::test]
    async fn preview_revision_fences_a_delayed_trace_grant_after_revoke() {
        let Some(store) = test_store().await else {
            return;
        };
        let workspace_id = format!("T{}", Uuid::new_v4().simple().to_string().to_uppercase());
        let user_id = format!("U{}", Uuid::new_v4().simple().to_string().to_uppercase());
        let expires_at = OffsetDateTime::now_utc() + TimeDuration::hours(1);
        let preview_revision = store
            .metadata_trace_consent("slack", &workspace_id, &user_id)
            .await
            .unwrap()
            .revision;
        let revoked = store
            .revoke_metadata_trace_consent("slack", &workspace_id, &user_id, "subject")
            .await
            .unwrap()
            .0;
        assert_eq!(preview_revision, 0);
        assert!(!revoked.enabled);
        assert!(matches!(
            store
                .grant_metadata_trace_consent_idempotent(
                    "slack",
                    &workspace_id,
                    &user_id,
                    expires_at,
                    Some(preview_revision),
                    "delayed-confirmation",
                    "put:delayed-confirmation",
                )
                .await,
            Err(SessionStoreError::MetadataTraceConsentRevisionChanged)
        ));
        assert_eq!(
            store
                .metadata_trace_consent("slack", &workspace_id, &user_id)
                .await
                .unwrap(),
            revoked
        );
    }

    #[tokio::test]
    async fn expired_consent_reports_off_and_can_be_renewed_from_its_revision() {
        let Some(store) = test_store().await else {
            return;
        };
        let workspace_id = format!("T-expired-{}", Uuid::new_v4());
        let user_id = format!("U-expired-{}", Uuid::new_v4());
        let expired = store
            .grant_metadata_trace_consent(
                "slack",
                &workspace_id,
                &user_id,
                OffsetDateTime::now_utc() - TimeDuration::seconds(1),
            )
            .await
            .unwrap();
        assert!(expired.enabled);

        let status = store
            .metadata_trace_consent("slack", &workspace_id, &user_id)
            .await
            .unwrap();
        assert!(!status.enabled, "elapsed grants must report as off");
        assert_eq!(status.expires_at, None);
        assert_eq!(status.revision, expired.revision);

        let expired_rows = store
            .expire_elapsed_metadata_trace_consents()
            .await
            .unwrap();
        assert_eq!(expired_rows.len(), 1);
        assert!(!expired_rows[0].enabled);
        assert_eq!(expired_rows[0].revision, expired.revision + 1);
        let durable = store
            .metadata_trace_consent("slack", &workspace_id, &user_id)
            .await
            .unwrap();
        assert_eq!(durable.revision, expired.revision + 1);

        let renewed = store
            .grant_metadata_trace_consent_idempotent(
                "slack",
                &workspace_id,
                &user_id,
                OffsetDateTime::now_utc() + TimeDuration::hours(1),
                Some(durable.revision),
                "renew-after-expiry",
                "put:renew-after-expiry",
            )
            .await
            .unwrap();
        assert!(renewed.enabled);
        assert_eq!(renewed.revision, durable.revision + 1);
        assert!(!renewed.drain_pending);

        sqlx::query(
            "delete from metadata_trace_consent_requests
             where source = 'slack' and workspace_id = $1 and user_id = $2",
        )
        .bind(&workspace_id)
        .bind(&user_id)
        .execute(store.pool())
        .await
        .unwrap();
        sqlx::query(
            "delete from metadata_trace_consents
             where source = 'slack' and workspace_id = $1 and user_id = $2",
        )
        .bind(&workspace_id)
        .bind(&user_id)
        .execute(store.pool())
        .await
        .unwrap();
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
                    &SandboxAssignmentSnapshot::unassigned(),
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
        assert!(!revoked.enabled);
        assert!(revoked.drain_pending);
        assert_eq!(
            store
                .metadata_trace_consent("slack", &workspace_id, &user_id)
                .await
                .unwrap(),
            revoked,
            "the acknowledged revoke must leave its exact drain durable"
        );
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
                    &SandboxAssignmentSnapshot::unassigned(),
                    &workspace_id,
                    &user_id,
                    "uid-new",
                )
                .await
                .unwrap()
        );
        assert!(
            store
                .metadata_trace_drain_targets_if_current(
                    "slack",
                    &workspace_id,
                    &user_id,
                    revoked.revision,
                )
                .await
                .unwrap()
                .is_none(),
            "a stale replica must not claim a completed revoke after regrant"
        );
        assert!(
            !store
                .complete_metadata_trace_drain_if_current_and_empty(
                    "slack",
                    &workspace_id,
                    &user_id,
                    revoked.revision,
                )
                .await
                .unwrap(),
            "a stale drain completion must not mutate the new grant"
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
        sqlx::query("delete from sessions where thread_key = $1")
            .bind(thread_key.as_str())
            .execute(store.pool())
            .await
            .unwrap();
        sqlx::query(
            "delete from metadata_trace_consent_requests
             where source = 'slack' and workspace_id = $1 and user_id = $2",
        )
        .bind(&workspace_id)
        .bind(&user_id)
        .execute(store.pool())
        .await
        .unwrap();
        sqlx::query(
            "delete from metadata_trace_consents
             where source = 'slack' and workspace_id = $1 and user_id = $2",
        )
        .bind(&workspace_id)
        .bind(&user_id)
        .execute(store.pool())
        .await
        .unwrap();
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
            metadata_trace_expires_at: Some(postgres_timestamp(
                OffsetDateTime::now_utc() + TimeDuration::hours(1),
            )),
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
                    &SandboxAssignmentSnapshot::unassigned(),
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
        // Workspace tests share one Postgres database across test binaries.
        // Fence the singleton config table during setup so another binary
        // cannot roll the generation between assignment and guard acquisition.
        let (identity, config_setup_fence) = loop {
            let identity = MetadataTraceConfigIdentity {
                generation: next_trace_generation(&store).await,
                fingerprint: format!("trace-guard-{}", Uuid::new_v4()),
                enabled: true,
            };
            match store.activate_metadata_trace_config(&identity).await {
                Ok(()) => {}
                Err(
                    SessionStoreError::StaleMetadataTraceGeneration { .. }
                    | SessionStoreError::MetadataTraceConfigConflict { .. },
                ) => continue,
                Err(error) => panic!("activate trace guard config: {error}"),
            }
            let mut fence = store.pool().begin().await.unwrap();
            sqlx::query("lock table metadata_trace_config_state in share mode")
                .execute(&mut *fence)
                .await
                .unwrap();
            let active = sqlx::query_scalar::<_, bool>(
                "select generation = $1 and config_fingerprint = $2 from metadata_trace_config_state where singleton = true",
            )
            .bind(identity.generation)
            .bind(&identity.fingerprint)
            .fetch_one(&mut *fence)
            .await
            .unwrap();
            if active {
                break (identity, fence);
            }
            fence.rollback().await.unwrap();
        };
        let expiry = postgres_timestamp(OffsetDateTime::now_utc() + TimeDuration::hours(1));
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
                    &SandboxAssignmentSnapshot::unassigned(),
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
        let assignment = store
            .metadata_trace_assignment_actor(&thread_key, "sbx-guard")
            .await
            .unwrap()
            .expect("persisted assignment actor");
        assert_eq!(assignment.source, "slack");
        assert_eq!(assignment.workspace_id, workspace_id);
        assert_eq!(assignment.user_id, user_id);
        assert_eq!(assignment.resource_uid, "uid-guard");
        assert!(!assignment.assignment_epoch.is_empty());
        let guard = store
            .lock_metadata_trace_input(
                &capabilities,
                &thread_key,
                &execution.execution.execution_id,
                "sbx-guard",
                &assignment.assignment_epoch,
                &assignment.resource_uid,
            )
            .await
            .unwrap()
            .expect("guard");
        config_setup_fence.commit().await.unwrap();

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
                    "sbx-guard",
                    &assignment.assignment_epoch,
                    &assignment.resource_uid,
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
                    &SandboxAssignmentSnapshot::unassigned(),
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
                    &SandboxAssignmentSnapshot::unassigned(),
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
                    &SandboxAssignmentSnapshot::unassigned(),
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
            resource_uid: Some("uid-idle-row".to_owned()),
            assignment_epoch: Some("epoch-idle-row".to_owned()),
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
            "successor creation must wait on the same session serialization lock"
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
        let successor_execution = match successor.await.expect("successor task completes") {
            Ok(created) => created.execution,
            Err(_) => {
                store
                    .create_execution(&thread_key, None, json!({"successor": true}))
                    .await
                    .expect("successor insert succeeds after terminal commit")
                    .execution
            }
        };
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
        let trace_capabilities = SandboxCapabilities::default_enabled();
        store
            .update_sandbox_assignment(&thread_key, "sbx-stdout-owner", None, &trace_capabilities)
            .await
            .expect("persist trace assignment");

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
        assert_eq!(root_a.sandbox_capabilities, Some(trace_capabilities));

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
            .insert_ready_warm_sandbox(&sandbox_id, Some("uid-warm"), &workload_key)
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

        assert_eq!(reserved.len(), 1);
        assert_eq!(reserved[0].sandbox_id, sandbox_id);
        assert_eq!(reserved[0].resource_uid.as_deref(), Some("uid-warm"));
        assert!(reserved[0].assignment_epoch.is_some());
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn warm_eviction_fence_preserves_a_same_name_replacement() {
        let Some(store) = test_store().await else {
            return;
        };
        let sandbox_id = format!("sbx-warm-aba-{}", Uuid::new_v4());
        let workload_key = format!("workload-warm-aba-{}", Uuid::new_v4());
        store
            .insert_ready_warm_sandbox(&sandbox_id, Some("uid-old"), &workload_key)
            .await
            .expect("insert warm sandbox");
        let old_epoch = sqlx::query_scalar::<_, String>(
            "select sandbox_assignment_epoch from session_warm_sandboxes where sandbox_id = $1",
        )
        .bind(&sandbox_id)
        .fetch_one(store.pool())
        .await
        .expect("read old warm identity");
        let reservation = ReadyWarmSandbox {
            sandbox_id: sandbox_id.clone(),
            resource_uid: Some("uid-old".to_owned()),
            assignment_epoch: Some(old_epoch),
        };

        sqlx::query(
            r#"
            update session_warm_sandboxes
            set sandbox_resource_uid = 'uid-new',
                sandbox_assignment_epoch = md5(random()::text || clock_timestamp()::text),
                status = 'ready'
            where sandbox_id = $1
            "#,
        )
        .bind(&sandbox_id)
        .execute(store.pool())
        .await
        .expect("replace same-name warm sandbox");

        assert!(
            !store
                .mark_warm_sandbox_failed_if_matches(&reservation, "stale eviction")
                .await
                .expect("fenced stale eviction")
        );
        let replacement_thread =
            ThreadKey::parse(format!("test:warm-aba-{}", Uuid::new_v4())).unwrap();
        store
            .create_or_get_session(
                &replacement_thread,
                &HarnessType::Codex,
                None,
                json!({}),
                Default::default(),
            )
            .await
            .expect("create replacement claimant");
        let replacement = store
            .claim_ready_warm_sandbox(&workload_key, replacement_thread.as_str())
            .await
            .expect("claim replacement")
            .expect("same-name replacement remains ready");
        assert_eq!(replacement.sandbox_id, sandbox_id);
        assert_eq!(replacement.resource_uid.as_deref(), Some("uid-new"));
    }
}
