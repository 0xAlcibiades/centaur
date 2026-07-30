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
    FromRow, PgPool,
    postgres::{PgListener, PgPoolOptions},
    types::Json,
};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

// The API binary embeds these migrations at compile time.
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

pub const SESSION_EVENTS_CHANNEL: &str = "centaur_session_events";
const DEFAULT_MAX_CONNECTIONS: u32 = 500;

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

    pub async fn run_migrations(&self) -> Result<(), SessionStoreError> {
        MIGRATOR.run(&self.pool).await?;
        Ok(())
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
            select thread_key, title, sandbox_id, sandbox_repo_cache_enabled, sandbox_repo_cache_access, sandbox_observability_enabled, sandbox_api_server_enabled, harness_type, harness_thread_id, persona_id, status, iron_control_principal, proxy_labels, sandbox_last_active_at, created_at, updated_at
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
                sandbox_last_active_at = case
                    when $2::text is null then null
                    else now()
                end,
                updated_at = now()
            where thread_key = $1
            returning thread_key, title, sandbox_id, sandbox_repo_cache_enabled, sandbox_repo_cache_access, sandbox_observability_enabled, sandbox_api_server_enabled, harness_type, harness_thread_id, persona_id, status, iron_control_principal, proxy_labels, sandbox_last_active_at, created_at, updated_at
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
                sandbox_last_active_at = now(),
                updated_at = now()
            where thread_key = $1
            returning thread_key, title, sandbox_id, sandbox_repo_cache_enabled, sandbox_repo_cache_access, sandbox_observability_enabled, sandbox_api_server_enabled, harness_type, harness_thread_id, persona_id, status, iron_control_principal, proxy_labels, sandbox_last_active_at, created_at, updated_at
            "#,
        )
        .bind(thread_key.as_str())
        .bind(sandbox_id)
        .bind(capabilities.repo_cache_enabled())
        .bind(capabilities.repo_cache.as_str())
        .bind(capabilities.observability_enabled)
        .bind(capabilities.api_server_enabled)
        .fetch_one(&self.pool)
        .await?;

        row.try_into()
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
                sandbox_last_active_at = null,
                status = $3,
                updated_at = now()
            where thread_key = $1
            returning thread_key, title, sandbox_id, sandbox_repo_cache_enabled, sandbox_repo_cache_access, sandbox_observability_enabled, sandbox_api_server_enabled, harness_type, harness_thread_id, persona_id, status, iron_control_principal, proxy_labels, sandbox_last_active_at, created_at, updated_at
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
            returning thread_key, title, sandbox_id, sandbox_repo_cache_enabled, sandbox_repo_cache_access, sandbox_observability_enabled, sandbox_api_server_enabled, harness_type, harness_thread_id, persona_id, status, iron_control_principal, proxy_labels, sandbox_last_active_at, created_at, updated_at
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
            returning thread_key, title, sandbox_id, sandbox_repo_cache_enabled, sandbox_repo_cache_access, sandbox_observability_enabled, sandbox_api_server_enabled, harness_type, harness_thread_id, persona_id, status, iron_control_principal, proxy_labels, sandbox_last_active_at, created_at, updated_at
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
struct SessionRow {
    thread_key: String,
    title: Option<String>,
    sandbox_id: Option<String>,
    sandbox_repo_cache_enabled: Option<bool>,
    sandbox_repo_cache_access: Option<String>,
    sandbox_observability_enabled: Option<bool>,
    sandbox_api_server_enabled: Option<bool>,
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
            ) {
                (
                    Some(repo_cache_enabled),
                    repo_cache_access,
                    Some(observability_enabled),
                    Some(api_server_enabled),
                ) => Some(SandboxCapabilities {
                    repo_cache: repo_cache_access
                        .as_deref()
                        .and_then(SandboxRepoCacheAccess::parse)
                        .unwrap_or_else(|| {
                            SandboxRepoCacheAccess::from_legacy_enabled(repo_cache_enabled)
                        }),
                    observability_enabled,
                    api_server_enabled,
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

    use centaur_session_core::{ExecutionStatus, HarnessType, SessionStatus, ThreadKey};
    use serde_json::json;
    use time::{Duration as TimeDuration, OffsetDateTime};
    use uuid::Uuid;

    use super::{
        IdleSandboxCandidateRow, OwnedTerminalEvent, PgSessionStore, SessionEventNotification,
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
