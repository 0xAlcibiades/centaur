mod cleanup;
mod title_generator;

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    future::Future,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime},
};

use centaur_iron_control::{SessionRegistrar, SlackTraceSubject};
use centaur_sandbox_core::{
    Mount, RepoCacheAccess, SandboxBackend, SandboxCapabilities as BackendSandboxCapabilities,
    SandboxError, SandboxHandle, SandboxId, SandboxIoGuard, SandboxRead, SandboxSpec,
    SandboxStatus, SandboxWrite,
};
use centaur_sandbox_manager::{
    SandboxManager, SandboxReaper, SandboxReaperConfig, WarmPoolConfig, WarmPoolError,
    WarmPoolManager, WarmSandboxSpecFactory,
};
use centaur_session_core::{
    ChatDestination, ExecutionStatus, HarnessType, MessageRole,
    SandboxCapabilities as SessionSandboxCapabilities,
    SandboxRepoCacheAccess as SessionRepoCacheAccess, Session, SessionEvent, SessionExecution,
    SessionMessageInput, ThreadKey,
};
use centaur_session_sqlx::{
    AppendMessagesWithoutActiveExecution, ClaimedInputDelivery, InputDeliveryState,
    MetadataTraceConfigIdentity, MetadataTraceConsent, MetadataTraceDrainTarget,
    MetadataTraceReconcilerLease, OwnedTerminalEvent, PgSessionStore, PreparedInputDelivery,
    PreparedSessionMessage, SandboxAssignmentIdentity, SandboxAssignmentReconciliationLock,
    SandboxAssignmentSnapshot, SandboxCapacityCandidate, SessionEventListener, SessionStoreError,
    default_metadata,
};
use centaur_telemetry::{
    export_thread_trace_root_span, record_sandbox_warm_pool_claim,
    record_session_execution_finished, record_session_execution_started, record_session_failure,
    record_session_first_token_latency, set_span_parent_trace,
};
use dashmap::{DashMap, DashSet};
use futures_util::{FutureExt, SinkExt, Stream, StreamExt, future::BoxFuture, stream};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::{
    io,
    sync::Mutex,
    time::{Instant, Interval, MissedTickBehavior, interval_at, sleep, timeout, timeout_at},
};
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec, LinesCodecError};
use tracing::{Instrument, Span, debug, error, info, info_span, warn};
use uuid::Uuid;

pub use cleanup::SessionSandboxCleanupConfig;
pub use title_generator::SessionTitleGenerationError;
use title_generator::{
    OpenAiSessionTitleGenerator, sanitize_session_title, session_title_source_from_parts,
};

pub const SESSION_OUTPUT_LINE_EVENT: &str = "session.output.line";
pub const SESSION_FIRST_TOKEN_EVENT: &str = "session.first_token";

const EVENT_STREAM_SAFETY_POLL_INTERVAL: Duration = Duration::from_secs(30);
const SESSION_PIPE_MAX_REATTACH_ATTEMPTS: u32 = 3;
const SESSION_PIPE_REATTACH_DELAY: Duration = Duration::from_millis(500);
const STDOUT_OWNER_LEASE: Duration = Duration::from_secs(45);
const STDOUT_OWNER_RENEW_INTERVAL: Duration = Duration::from_secs(10);
const COMPLETED_OUTPUT_ID_CAPACITY: usize = 256;
#[cfg(not(test))]
const EXECUTION_ADOPTION_IO_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
const EXECUTION_ADOPTION_IO_TIMEOUT: Duration = Duration::from_secs(2);
const EXECUTION_HANDOFF_POLL_INTERVAL: Duration = Duration::from_millis(500);
const EXECUTION_HANDOFF_DB_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(not(test))]
const STDOUT_OWNER_RELEASE_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(test)]
const STDOUT_OWNER_RELEASE_TIMEOUT: Duration = Duration::from_millis(250);
#[cfg(not(test))]
const STDOUT_OWNER_RENEWER_STOP_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(test)]
const STDOUT_OWNER_RENEWER_STOP_TIMEOUT: Duration = Duration::from_millis(250);
/// A live execution can briefly have no sandbox while it moves from queued
/// through warm-sandbox assignment. A periodic adoption scan must not fail a
/// young row it observes in that window.
const PRE_SANDBOX_ORPHAN_GRACE: Duration = Duration::from_secs(120);
const COMPONENT_SESSION_RUNTIME: &str = "session_runtime";
#[cfg_attr(test, allow(dead_code))]
const METADATA_TRACE_INPUT_WRITE_MAX: Duration = Duration::from_secs(30);

#[cfg(test)]
static METADATA_TRACE_INPUT_TEST_TIMEOUT_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(30_000);
const SANDBOX_REPOS_MOUNT_PATH: &str = "/home/agent/github";
const PUBLIC_REPO_CACHE_SUBPATH: &str = "public";
const CENTAUR_SKILL_DIRS_ENV: &str = "CENTAUR_SKILL_DIRS";
const CENTAUR_PUBLIC_SKILL_DIRS_ENV: &str = "CENTAUR_PUBLIC_SKILL_DIRS";
const SANDBOX_REPO_CACHE_LABEL: &str = "centaur.sandbox_repo_cache";
const OBSERVABILITY_TOOL_BLOCKLIST: &str =
    "vlogs,vmetrics,grafana,centaur_investigator,centaur-investigator";
const MAX_METADATA_TRACE_CONSENT: TimeDuration = TimeDuration::hours(24);
const METADATA_TRACE_RECONCILER_LEASE: TimeDuration = TimeDuration::seconds(20);
/// Assignment-row locks deliberately span backend observation so a successor
/// cannot start on a resource whose identity is being reconciled.
#[cfg(not(test))]
const ASSIGNMENT_RECONCILIATION_BACKEND_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
static ASSIGNMENT_RECONCILIATION_BACKEND_TIMEOUT_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(10_000);

fn assignment_reconciliation_backend_timeout() -> Duration {
    #[cfg(test)]
    {
        Duration::from_millis(ASSIGNMENT_RECONCILIATION_BACKEND_TIMEOUT_MS.load(Ordering::Relaxed))
    }
    #[cfg(not(test))]
    {
        ASSIGNMENT_RECONCILIATION_BACKEND_TIMEOUT
    }
}

type SandboxSpecFactory = Arc<
    dyn Fn(&ThreadKey, &str, &HarnessType, Option<&PersonaContext>) -> SandboxSpec + Send + Sync,
>;
type SessionInputSink = FramedWrite<SandboxWrite, LinesCodec>;
type ExecutionSpanRegistry = Arc<Mutex<HashMap<String, Span>>>;
type SharedStdoutPumpState = Arc<Mutex<StdoutPumpState>>;
type SessionPipeMap = Arc<DashMap<String, SessionPipe>>;
type WeakLockRegistry<T> = Arc<DashMap<String, Weak<RegisteredLock<T>>>>;
type SharedRegisteredLock<T> = Arc<RegisteredLock<T>>;
type SessionPipeOpenLocks = WeakLockRegistry<Mutex<()>>;
type SessionOutputGates = WeakLockRegistry<tokio::sync::RwLock<()>>;
type SessionPipeOpenLock = SharedRegisteredLock<Mutex<()>>;
type SessionOutputGate = SharedRegisteredLock<tokio::sync::RwLock<()>>;
type ToolHostCallLocks = Arc<DashMap<String, Arc<Mutex<()>>>>;
type SessionTitleThreadSet = Arc<DashSet<ThreadKey>>;
type StdoutOwnerRenewalRegistry = Arc<DashMap<String, Arc<StdoutOwnerRenewal>>>;
type SessionTitleGenerator = Arc<
    dyn Fn(String) -> BoxFuture<'static, Result<String, SessionTitleGenerationError>> + Send + Sync,
>;

struct StdoutOwnerRenewal {
    generation: Uuid,
    cancel: tokio::sync::watch::Sender<bool>,
    stopped: tokio::sync::watch::Sender<bool>,
    #[cfg(test)]
    renew_now: tokio::sync::Notify,
    #[cfg(test)]
    renew_db_started: tokio::sync::Notify,
}

impl StdoutOwnerRenewal {
    async fn wait_stopped(&self) {
        let mut stopped = self.stopped.subscribe();
        while !*stopped.borrow_and_update() {
            if stopped.changed().await.is_err() {
                return;
            }
        }
    }
}

struct RegisteredLock<T> {
    key: String,
    lock: T,
    registry: Weak<DashMap<String, Weak<RegisteredLock<T>>>>,
}

impl<T> std::ops::Deref for RegisteredLock<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.lock
    }
}

impl<T> Drop for RegisteredLock<T> {
    fn drop(&mut self) {
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        let self_ptr = self as *const Self;
        registry.remove_if(&self.key, |_, current| {
            std::ptr::eq(current.as_ptr(), self_ptr)
        });
    }
}

#[derive(Clone)]
pub struct SessionRuntime {
    store: PgSessionStore,
    sandbox_runtime: SandboxRuntime,
    sandbox_pipes: SessionPipeMap,
    sandbox_pipe_open_locks: SessionPipeOpenLocks,
    sandbox_output_gates: SessionOutputGates,
    tool_host_call_locks: ToolHostCallLocks,
    execution_spans: ExecutionSpanRegistry,
    iron_control: Option<SessionRegistrar>,
    metadata_trace_config: Option<MetadataTraceConfigIdentity>,
    metadata_trace_reconciler_owner_id: String,
    warm_pool: Option<Arc<WarmPoolManager>>,
    personas: Option<Arc<PersonaRegistry>>,
    session_title_generator: Option<SessionTitleGenerator>,
    session_title_in_flight: SessionTitleThreadSet,
    session_title_rerun_requested: SessionTitleThreadSet,
    capacity: Option<Arc<SandboxCapacityController>>,
    stdout_owner_id: String,
    stdout_owner_claim_gate: Arc<Mutex<()>>,
    stdout_owner_renewals: StdoutOwnerRenewalRegistry,
    /// Set once a shutdown handoff begins; fences new stdout-owner claims
    /// so an execution cannot start on a control plane that is about to
    /// exit and release its leases.
    shutting_down: Arc<AtomicBool>,
    #[cfg(test)]
    fail_adoption_root_persistence: Arc<AtomicBool>,
    #[cfg(test)]
    stdout_owner_claim_db_started: Arc<tokio::sync::Notify>,
    #[cfg(test)]
    stdout_owner_release_started: Arc<tokio::sync::Notify>,
}

#[derive(Clone, Copy, Debug)]
pub struct SandboxCapacityConfig {
    pub max_running: usize,
    pub hot_idle_grace: Duration,
}

impl SandboxCapacityConfig {
    pub fn is_enabled(&self) -> bool {
        self.max_running > 0
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PersonaRegistry {
    personas: BTreeMap<String, PersonaDefinition>,
    default_persona_id: Option<String>,
    overlay_chain: Vec<String>,
    public_source_roots: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PersonaDefinition {
    pub id: String,
    pub source_root: String,
    pub source_path: String,
    pub source_ref: Option<String>,
    pub prompt_hash: String,
    #[serde(skip_serializing)]
    pub prompt: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PersonaSummary {
    pub id: String,
    pub source_root: String,
    pub source_path: String,
    pub source_ref: Option<String>,
    pub prompt_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PersonaContext {
    pub persona_id: String,
    pub source_root: String,
    pub source_path: String,
    pub source_ref: Option<String>,
    pub prompt_hash: String,
    pub defaulted: bool,
    pub overlay_chain: Vec<String>,
}

impl PersonaRegistry {
    pub fn new(
        personas: impl IntoIterator<Item = PersonaDefinition>,
        default_persona_id: Option<String>,
        overlay_chain: Vec<String>,
    ) -> Result<Self, String> {
        let personas = personas
            .into_iter()
            .map(|persona| (persona.id.clone(), persona))
            .collect::<BTreeMap<_, _>>();
        if let Some(default_persona_id) = default_persona_id.as_deref()
            && !personas.contains_key(default_persona_id)
        {
            return Err(format!(
                "CENTAUR_DEFAULT_PERSONA {default_persona_id:?} is not in the deployed persona registry"
            ));
        }
        Ok(Self {
            personas,
            default_persona_id,
            overlay_chain,
            public_source_roots: BTreeSet::new(),
        })
    }

    pub fn with_public_source_roots(
        mut self,
        public_source_roots: impl IntoIterator<Item = String>,
    ) -> Self {
        self.public_source_roots = public_source_roots.into_iter().collect();
        self
    }

    pub fn summaries(&self) -> Vec<PersonaSummary> {
        self.personas
            .values()
            .map(|persona| PersonaSummary {
                id: persona.id.clone(),
                source_root: persona.source_root.clone(),
                source_path: persona.source_path.clone(),
                source_ref: persona.source_ref.clone(),
                prompt_hash: persona.prompt_hash.clone(),
            })
            .collect()
    }

    fn default_persona_id(&self) -> Option<&str> {
        self.default_persona_id.as_deref()
    }

    fn default_persona_id_for_access(&self, access: &SessionRepoCacheAccess) -> Option<&str> {
        let default_persona_id = self.default_persona_id()?;
        let persona = self.get(default_persona_id)?;
        if self.persona_allowed_for_access(persona, access) {
            Some(default_persona_id)
        } else {
            None
        }
    }

    fn get(&self, persona_id: &str) -> Option<&PersonaDefinition> {
        self.personas.get(persona_id)
    }

    fn persona_allowed_for_access(
        &self,
        persona: &PersonaDefinition,
        access: &SessionRepoCacheAccess,
    ) -> bool {
        !matches!(access, SessionRepoCacheAccess::Public)
            || self.public_source_roots.contains(&persona.source_root)
    }

    fn context_for_access(
        &self,
        persona_id: &str,
        defaulted: bool,
        access: &SessionRepoCacheAccess,
    ) -> Result<PersonaContext, String> {
        let Some(persona) = self.get(persona_id) else {
            return Err(format!(
                "persona {persona_id:?} is not available in this deployment"
            ));
        };
        if !self.persona_allowed_for_access(persona, access) {
            return Err(format!(
                "persona {persona_id:?} is not available for public sandbox repo-cache access"
            ));
        }
        Ok(PersonaContext {
            persona_id: persona.id.clone(),
            source_root: persona.source_root.clone(),
            source_path: persona.source_path.clone(),
            source_ref: persona.source_ref.clone(),
            prompt_hash: persona.prompt_hash.clone(),
            defaulted,
            overlay_chain: self.overlay_chain.clone(),
        })
    }
}

#[derive(Clone)]
pub struct SandboxRuntime {
    manager: Arc<SandboxManager>,
    spec_factory: SandboxSpecFactory,
    warm_spec_factory: Option<WarmSandboxSpecFactory>,
    workload_key: Option<String>,
    /// The harness warm sandboxes boot with. A warm claim is only valid for a
    /// session on the same harness; other sessions get a cold sandbox.
    warm_harness: Option<HarnessType>,
}

#[derive(Clone, Debug)]
pub enum SandboxWorkloadMode {
    MockAppServer {
        image: String,
    },
    CodexAppServer {
        image: String,
        env: Vec<(String, String)>,
        mounts: Vec<Mount>,
        /// The harness used for warm sandboxes and as the workload default.
        /// Per-session sandboxes run the session's own harness.
        harness: HarnessType,
    },
}

/// What to do when a session already exists with a different harness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HarnessConflictPolicy {
    /// Fail with [`SessionStoreError::HarnessConflict`] (the default).
    Reject,
    /// Restart the thread on the requested harness: stop the old sandbox,
    /// clear the harness thread state, and switch the session row over. The
    /// new harness starts with no conversational memory.
    Restart,
}

/// Result of [`SessionRuntime::create_or_get_session`].
#[derive(Clone, Debug)]
pub struct CreateOrGetSessionOutcome {
    pub session: Session,
    /// True when the session was restarted onto a different harness because
    /// the request asked for [`HarnessConflictPolicy::Restart`].
    pub harness_switched: bool,
}

/// Outcome of [`SessionRuntime::drain`]: the sandboxes that were stopped and
/// any that failed to stop (with the backend error text).
#[derive(Debug, Default)]
pub struct DrainReport {
    pub stopped: Vec<String>,
    pub failed: Vec<DrainFailure>,
}

#[derive(Debug)]
pub struct DrainFailure {
    pub sandbox_id: String,
    pub error: String,
}

#[derive(Debug, Default)]
pub struct WorkflowSandboxCleanupReport {
    pub stopped: Vec<String>,
    pub missing: Vec<String>,
    pub failed: Vec<DrainFailure>,
}

#[derive(Debug)]
pub struct ExecuteSessionInput {
    pub idempotency_key: Option<String>,
    pub metadata: Option<Value>,
    pub input_lines: Vec<String>,
    pub idle_timeout_ms: Option<u64>,
    pub max_duration_ms: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct InterruptExecutionOutcome {
    pub interrupted: bool,
    pub execution_id: Option<String>,
}

#[derive(Debug)]
pub struct ToolHostCallInput {
    pub principal_id: String,
    pub token_id: Option<String>,
    pub tool_name: String,
    pub method: String,
    pub arguments: Value,
    pub timeout: Duration,
}

#[derive(Debug)]
pub struct ToolHostCallOutput {
    pub sandbox_id: String,
    pub stdout: String,
    pub stderr: String,
    pub exit_status: Option<i32>,
    pub timed_out: bool,
}

#[derive(Clone)]
struct SessionPipe {
    stdin: Arc<Mutex<SessionInputSink>>,
    output_state: SharedStdoutPumpState,
    output_gate: SessionOutputGate,
    stdout_alive: Arc<AtomicBool>,
    assignment_epoch: Option<String>,
    resource_uid: Option<String>,
    trace_assignment_epoch: Option<String>,
    trace_resource_uid: Option<String>,
    #[cfg(test)]
    output_gate_read_wait_started: Arc<tokio::sync::Notify>,
}

impl SessionPipe {
    async fn output_read_guard(&self) -> tokio::sync::RwLockReadGuard<'_, ()> {
        #[cfg(not(test))]
        {
            self.output_gate.read().await
        }
        #[cfg(test)]
        {
            let read = self.output_gate.read();
            tokio::pin!(read);
            let mut wait_reported = false;
            futures_util::future::poll_fn(|cx| {
                let poll = read.as_mut().poll(cx);
                if poll.is_pending() && !wait_reported {
                    self.output_gate_read_wait_started.notify_one();
                    wait_reported = true;
                }
                poll
            })
            .await
        }
    }
}

#[derive(Serialize)]
struct ToolHostRequest {
    id: String,
    tool: String,
    method: String,
    arguments: Value,
    principal_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_id: Option<String>,
    timeout_seconds: u64,
}

#[derive(Deserialize)]
struct ToolHostResponse {
    status: Option<i32>,
    #[serde(default)]
    stdout: String,
    #[serde(default)]
    stderr: String,
    #[serde(default)]
    timed_out: bool,
}

/// Shared handles threaded through background session tasks (stdout pump,
/// terminal-output recording, max-duration failure, idle pause).
#[derive(Clone)]
struct RuntimeContext {
    store: PgSessionStore,
    manager: Arc<SandboxManager>,
    sandbox_pipes: SessionPipeMap,
    execution_spans: ExecutionSpanRegistry,
    stdout_owner_id: String,
    stdout_owner_renewals: StdoutOwnerRenewalRegistry,
}

struct SandboxCapacityController {
    store: PgSessionStore,
    manager: Arc<SandboxManager>,
    sandbox_pipes: SessionPipeMap,
    lock: Mutex<()>,
    config: SandboxCapacityConfig,
}

impl SandboxCapacityController {
    fn new(
        store: PgSessionStore,
        manager: Arc<SandboxManager>,
        sandbox_pipes: SessionPipeMap,
        config: SandboxCapacityConfig,
    ) -> Self {
        Self {
            store,
            manager,
            sandbox_pipes,
            lock: Mutex::new(()),
            config,
        }
    }

    async fn run_with_capacity<T, F, Fut>(
        &self,
        protected_thread_key: &ThreadKey,
        trigger_execution_id: &str,
        operation: &'static str,
        action: F,
    ) -> Result<T, SessionRuntimeError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, SessionRuntimeError>>,
    {
        let _guard = self.lock.lock().await;
        self.ensure_running_slot(protected_thread_key, trigger_execution_id, operation)
            .await?;
        action().await
    }

    async fn run_reuse_fence_with_capacity<T, F, Fut>(
        &self,
        sandbox_id: &SandboxId,
        protected_thread_key: &ThreadKey,
        trigger_execution_id: &str,
        action: F,
    ) -> Result<T, SessionRuntimeError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, SessionRuntimeError>>,
    {
        let _guard = self.lock.lock().await;
        match self.manager.status(sandbox_id).await {
            Ok(SandboxStatus::Running | SandboxStatus::Created) => {}
            Ok(SandboxStatus::Suspended) => {
                self.ensure_running_slot(protected_thread_key, trigger_execution_id, "resume")
                    .await?;
            }
            Ok(_) | Err(SandboxError::NotFound(_)) => {
                return Err(SessionRuntimeError::SandboxAssignmentChanged);
            }
            Err(error) => return Err(SessionRuntimeError::Sandbox(error)),
        }
        action().await
    }

    async fn ensure_running_slot(
        &self,
        protected_thread_key: &ThreadKey,
        trigger_execution_id: &str,
        operation: &'static str,
    ) -> Result<(), SessionRuntimeError> {
        let running = self.running_slot_count().await?;
        if running < self.config.max_running {
            return Ok(());
        }

        let mut slots_needed = running.saturating_sub(self.config.max_running) + 1;
        let mut stopped_warm = 0usize;
        let mut paused_idle = 0usize;
        let mut stale_candidates_reconciled = 0usize;

        for sandbox in self
            .store
            .reserve_ready_warm_sandboxes_for_eviction(candidate_fetch_limit(slots_needed))
            .await?
        {
            if slots_needed == 0 {
                break;
            }
            let sandbox_id = sandbox.sandbox_id.clone();
            let id = SandboxId::new(sandbox_id.as_str());
            match self.manager.observe(&id).await {
                Ok(observation)
                    if status_consumes_running_slot(&observation.status)
                        && observation.resource_uid == sandbox.resource_uid => {}
                Ok(_) | Err(SandboxError::NotFound(_)) => {
                    let _ = self
                        .store
                        .mark_warm_sandbox_failed_if_matches(
                            &sandbox,
                            "not running during sandbox capacity admission",
                        )
                        .await;
                    continue;
                }
                Err(error) => {
                    let failure =
                        format!("status failed during sandbox capacity admission: {error}");
                    let _ = self
                        .store
                        .mark_warm_sandbox_failed_if_matches(&sandbox, &failure)
                        .await;
                    return Err(SessionRuntimeError::Sandbox(error));
                }
            }

            let Some(resource_uid) = sandbox.resource_uid.as_deref() else {
                let _ = self
                    .store
                    .mark_warm_sandbox_failed_if_matches(
                        &sandbox,
                        "warm sandbox capacity eviction requires a stable resource UID",
                    )
                    .await;
                continue;
            };
            match self.manager.stop_exact(&id, Some(resource_uid)).await {
                Ok(()) | Err(SandboxError::NotFound(_)) => {
                    stopped_warm += 1;
                    slots_needed -= 1;
                    let _ = self
                        .store
                        .mark_warm_sandbox_failed_if_matches(
                            &sandbox,
                            "stopped for sandbox capacity pressure",
                        )
                        .await;
                    info!(
                        component = COMPONENT_SESSION_RUNTIME,
                        event = "sandbox_capacity_warm_stopped",
                        sandbox_id,
                        trigger_thread_key = %protected_thread_key,
                        trigger_execution_id,
                        operation,
                        max_running = self.config.max_running,
                        "stopped warm sandbox for capacity"
                    );
                }
                Err(error) => {
                    let failure = format!("stop failed during sandbox capacity admission: {error}");
                    let _ = self
                        .store
                        .mark_warm_sandbox_failed_if_matches(&sandbox, &failure)
                        .await;
                    return Err(SessionRuntimeError::Sandbox(error));
                }
            }
        }

        if slots_needed > 0 {
            loop {
                let candidates = self
                    .store
                    .list_sandbox_capacity_candidates(
                        Some(protected_thread_key),
                        self.config.hot_idle_grace,
                        candidate_fetch_limit(slots_needed),
                    )
                    .await?;
                if candidates.is_empty() {
                    break;
                }

                let mut made_progress = false;
                for candidate in candidates {
                    if slots_needed == 0 {
                        break;
                    }
                    match self
                        .pause_capacity_candidate(
                            &candidate,
                            protected_thread_key,
                            trigger_execution_id,
                            operation,
                        )
                        .await?
                    {
                        CapacityCandidateAction::Paused => {
                            paused_idle += 1;
                            slots_needed -= 1;
                            made_progress = true;
                        }
                        CapacityCandidateAction::ReconciledStale => {
                            stale_candidates_reconciled += 1;
                            made_progress = true;
                        }
                        CapacityCandidateAction::Skipped => {}
                    }
                }

                if slots_needed == 0 || !made_progress {
                    break;
                }
            }
        }

        if slots_needed == 0 {
            info!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "sandbox_capacity_admitted",
                trigger_thread_key = %protected_thread_key,
                trigger_execution_id,
                operation,
                running_before = running,
                max_running = self.config.max_running,
                stopped_warm,
                paused_idle,
                stale_candidates_reconciled,
                "admitted sandbox operation under capacity pressure"
            );
            return Ok(());
        }

        Err(SessionRuntimeError::CapacityExceeded {
            max_running: self.config.max_running,
            running,
            operation,
        })
    }

    async fn pause_capacity_candidate(
        &self,
        candidate: &SandboxCapacityCandidate,
        protected_thread_key: &ThreadKey,
        trigger_execution_id: &str,
        operation: &'static str,
    ) -> Result<CapacityCandidateAction, SessionRuntimeError> {
        let Some(resource_uid) = candidate.resource_uid.as_deref() else {
            return Ok(CapacityCandidateAction::Skipped);
        };
        let Some(assignment_epoch) = candidate.assignment_epoch.as_deref() else {
            return Ok(CapacityCandidateAction::Skipped);
        };
        let Some(mut assignment_lock) = self
            .store
            .lock_sandbox_assignment_for_reconciliation(
                &candidate.thread_key,
                &candidate.sandbox_id,
            )
            .await?
        else {
            return Ok(CapacityCandidateAction::Skipped);
        };
        if assignment_lock.resource_uid() != Some(resource_uid)
            || assignment_lock.assignment_epoch() != Some(assignment_epoch)
        {
            assignment_lock.rollback().await?;
            return Ok(CapacityCandidateAction::Skipped);
        }
        if !assignment_lock
            .is_idle_without_active_execution(self.config.hot_idle_grace)
            .await?
        {
            assignment_lock.rollback().await?;
            return Ok(CapacityCandidateAction::Skipped);
        }
        let id = SandboxId::new(candidate.sandbox_id.as_str());
        match observe_assignment_reconciliation(&self.manager, &id).await {
            Ok(observation)
                if observation.resource_uid.as_deref() == Some(resource_uid)
                    && matches!(
                        observation.status,
                        SandboxStatus::Running | SandboxStatus::Created | SandboxStatus::Unknown(_)
                    ) => {}
            Ok(observation)
                if observation.resource_uid.as_deref() == Some(resource_uid)
                    && observation.status == SandboxStatus::Suspended =>
            {
                assignment_lock.rollback().await?;
                return Ok(CapacityCandidateAction::Skipped);
            }
            Ok(observation) if observation.status.is_terminal() => {
                return self
                    .reconcile_stale_capacity_candidate(candidate, assignment_lock)
                    .await;
            }
            Err(SandboxError::NotFound(_)) => {
                return self
                    .reconcile_stale_capacity_candidate(candidate, assignment_lock)
                    .await;
            }
            Ok(_) => {
                assignment_lock.rollback().await?;
                return Ok(CapacityCandidateAction::Skipped);
            }
            Err(error) => {
                assignment_lock.rollback().await?;
                return Err(SessionRuntimeError::Sandbox(error));
            }
        }

        match pause_assignment_reconciliation(&self.manager, &id, resource_uid).await {
            Ok(()) => {
                if !assignment_lock.commit_if_current().await? {
                    return Ok(CapacityCandidateAction::Skipped);
                }
                remove_pipe_for_assignment(
                    &self.sandbox_pipes,
                    candidate.sandbox_id.as_str(),
                    resource_uid,
                    assignment_epoch,
                );
                self.store
                    .append_event(
                        &candidate.thread_key,
                        candidate.latest_execution_id.as_deref(),
                        "session.sandbox_paused",
                        json!({
                            "thread_key": candidate.thread_key.as_str(),
                            "sandbox_id": candidate.sandbox_id.as_str(),
                            "reason": "capacity_pressure",
                            "trigger_thread_key": protected_thread_key.as_str(),
                            "trigger_execution_id": trigger_execution_id,
                            "operation": operation,
                            "last_active_at": candidate.last_active_at,
                            "hot_idle_grace_ms": duration_millis_u64(self.config.hot_idle_grace),
                            "max_running": self.config.max_running,
                        }),
                    )
                    .await?;
                info!(
                    component = COMPONENT_SESSION_RUNTIME,
                    event = "sandbox_capacity_idle_paused",
                    thread_key = %candidate.thread_key,
                    sandbox_id = %candidate.sandbox_id,
                    trigger_thread_key = %protected_thread_key,
                    trigger_execution_id,
                    operation,
                    last_active_at = %candidate.last_active_at,
                    max_running = self.config.max_running,
                    "paused idle sandbox for capacity"
                );
                Ok(CapacityCandidateAction::Paused)
            }
            Err(error) => {
                assignment_lock.rollback().await?;
                self.store
                    .append_event(
                        &candidate.thread_key,
                        candidate.latest_execution_id.as_deref(),
                        "session.sandbox_pause_failed",
                        json!({
                            "thread_key": candidate.thread_key.as_str(),
                            "sandbox_id": candidate.sandbox_id.as_str(),
                            "reason": "capacity_pressure",
                            "trigger_thread_key": protected_thread_key.as_str(),
                            "trigger_execution_id": trigger_execution_id,
                            "operation": operation,
                            "error": error.to_string(),
                        }),
                    )
                    .await?;
                Err(SessionRuntimeError::Sandbox(error))
            }
        }
    }

    async fn reconcile_stale_capacity_candidate(
        &self,
        candidate: &SandboxCapacityCandidate,
        assignment_lock: SandboxAssignmentReconciliationLock<'_>,
    ) -> Result<CapacityCandidateAction, SessionRuntimeError> {
        let resource_uid = candidate
            .resource_uid
            .as_deref()
            .ok_or(SessionRuntimeError::SandboxAssignmentChanged)?;
        let assignment_epoch = candidate
            .assignment_epoch
            .as_deref()
            .ok_or(SessionRuntimeError::SandboxAssignmentChanged)?;
        let cleared = assignment_lock.clear_and_commit().await?;
        if cleared {
            remove_pipe_for_assignment(
                &self.sandbox_pipes,
                candidate.sandbox_id.as_str(),
                resource_uid,
                assignment_epoch,
            );
            info!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "sandbox_capacity_stale_reconciled",
                thread_key = %candidate.thread_key,
                sandbox_id = %candidate.sandbox_id,
                "cleared stale sandbox assignment during capacity admission"
            );
            Ok(CapacityCandidateAction::ReconciledStale)
        } else {
            Ok(CapacityCandidateAction::Skipped)
        }
    }

    async fn running_slot_count(&self) -> Result<usize, SessionRuntimeError> {
        Ok(self
            .manager
            .list_observed()
            .await?
            .into_iter()
            .filter(|observed| status_consumes_running_slot(&observed.status))
            .count())
    }
}

enum CapacityCandidateAction {
    Paused,
    ReconciledStale,
    Skipped,
}

fn candidate_fetch_limit(slots_needed: usize) -> i64 {
    slots_needed.saturating_mul(4).clamp(16, 1000) as i64
}

fn status_consumes_running_slot(status: &SandboxStatus) -> bool {
    matches!(
        status,
        SandboxStatus::Created | SandboxStatus::Running | SandboxStatus::Unknown(_)
    )
}

struct EventStreamState {
    store: PgSessionStore,
    thread_key: ThreadKey,
    after_event_id: i64,
    execution_id: Option<String>,
    pending: VecDeque<SessionEvent>,
    listener: SessionEventListener,
    safety_tick: Interval,
    done: bool,
    emitted_count: u64,
    span: Span,
}

struct SandboxReadyObservation<'a> {
    thread_key: &'a ThreadKey,
    execution_id: &'a str,
    sandbox_id: &'a str,
    harness_type: &'a HarnessType,
    source: &'static str,
    ready_duration: Duration,
    startup_duration: Option<Duration>,
}

struct EnsureSessionSandboxRequest<'a> {
    thread_key: &'a ThreadKey,
    harness_type: &'a HarnessType,
    persona_id: Option<&'a str>,
    existing_sandbox_id: Option<&'a str>,
    existing_sandbox_capabilities: Option<&'a SessionSandboxCapabilities>,
    iron_control_principal: Option<&'a str>,
    proxy_labels: &'a BTreeMap<String, String>,
    desired_capabilities: &'a SessionSandboxCapabilities,
    execution_metadata: Option<&'a Value>,
    execution_id: &'a str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SandboxBootMode {
    Harness,
    ToolHost { principal_id: String },
}

impl SandboxBootMode {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Harness => "harness",
            Self::ToolHost { .. } => "tool_host",
        }
    }

    fn uses_warm_pool(&self) -> bool {
        matches!(self, Self::Harness)
    }
}

struct PersonaResolution {
    persona_id: Option<String>,
    context: Option<PersonaContext>,
    defaulted: bool,
}

impl SessionRuntime {
    pub fn new(store: PgSessionStore, sandbox_runtime: SandboxRuntime) -> Self {
        Self {
            store,
            sandbox_runtime,
            sandbox_pipes: Arc::new(DashMap::new()),
            sandbox_pipe_open_locks: Arc::new(DashMap::new()),
            sandbox_output_gates: Arc::new(DashMap::new()),
            tool_host_call_locks: Arc::new(DashMap::new()),
            execution_spans: Arc::new(Mutex::new(HashMap::new())),
            iron_control: None,
            metadata_trace_config: None,
            metadata_trace_reconciler_owner_id: Uuid::new_v4().to_string(),
            warm_pool: None,
            personas: None,
            session_title_generator: None,
            session_title_in_flight: Arc::new(DashSet::new()),
            session_title_rerun_requested: Arc::new(DashSet::new()),
            capacity: None,
            stdout_owner_id: format!("api-rs-{}", uuid::Uuid::new_v4().simple()),
            stdout_owner_claim_gate: Arc::new(Mutex::new(())),
            stdout_owner_renewals: Arc::new(DashMap::new()),
            shutting_down: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            fail_adoption_root_persistence: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            stdout_owner_claim_db_started: Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            stdout_owner_release_started: Arc::new(tokio::sync::Notify::new()),
        }
    }

    pub fn with_session_title_generator<F, Fut>(mut self, generator: F) -> Self
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, SessionTitleGenerationError>> + Send + 'static,
    {
        self.session_title_generator = Some(Arc::new(move |source| generator(source).boxed()));
        self
    }

    pub fn with_openai_session_title_generator_from_env(mut self) -> Self {
        let Some(generator) = OpenAiSessionTitleGenerator::from_env() else {
            return self;
        };
        self.session_title_generator = Some(Arc::new(move |source| {
            let generator = generator.clone();
            async move { generator.generate(source).await }.boxed()
        }));
        self
    }

    pub fn with_personas(mut self, personas: PersonaRegistry) -> Self {
        self.personas = Some(Arc::new(personas));
        self
    }

    pub fn personas(&self) -> Vec<PersonaSummary> {
        self.personas
            .as_ref()
            .map(|personas| personas.summaries())
            .unwrap_or_default()
    }

    pub async fn session_title(
        &self,
        thread_key: &ThreadKey,
    ) -> Result<Option<String>, SessionRuntimeError> {
        Ok(self.store.get_session_title(thread_key).await?)
    }

    /// Returns the harness already persisted for a thread, if the session
    /// exists. API policy uses this to keep rollout assignments sticky across
    /// configuration changes without exposing the session store itself.
    pub async fn existing_session_harness(
        &self,
        thread_key: &ThreadKey,
    ) -> Result<Option<HarnessType>, SessionRuntimeError> {
        match self.store.get_session(thread_key).await {
            Ok(session) => Ok(Some(session.harness_type)),
            Err(SessionStoreError::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn resolve_persona_for_create(
        &self,
        requested_persona_id: Option<&str>,
        capabilities: &SessionSandboxCapabilities,
    ) -> Result<PersonaResolution, SessionRuntimeError> {
        let requested = requested_persona_id.and_then(clean_persona_id);
        let selected = requested.or_else(|| self.default_persona_id_for_access(capabilities));
        let defaulted = requested.is_none() && selected.is_some();
        let context = self.resolve_persona_context(selected, defaulted, capabilities)?;
        Ok(PersonaResolution {
            persona_id: selected.map(str::to_owned),
            context,
            defaulted,
        })
    }

    fn resolve_stored_persona(
        &self,
        persona_id: Option<&str>,
        _harness_type: &HarnessType,
        capabilities: &SessionSandboxCapabilities,
    ) -> Result<Option<PersonaContext>, SessionRuntimeError> {
        self.resolve_persona_context(persona_id.and_then(clean_persona_id), false, capabilities)
    }

    fn resolve_persona_context(
        &self,
        persona_id: Option<&str>,
        defaulted: bool,
        capabilities: &SessionSandboxCapabilities,
    ) -> Result<Option<PersonaContext>, SessionRuntimeError> {
        let Some(persona_id) = persona_id else {
            return Ok(None);
        };
        let Some(registry) = self.personas.as_ref() else {
            return Err(SessionRuntimeError::BadRequest(format!(
                "persona {persona_id:?} was requested but no persona registry is configured"
            )));
        };
        registry
            .context_for_access(persona_id, defaulted, &capabilities.repo_cache)
            .map(Some)
            .map_err(SessionRuntimeError::BadRequest)
    }

    fn default_persona_id(&self) -> Option<&str> {
        self.personas
            .as_ref()
            .and_then(|personas| personas.default_persona_id())
    }

    fn default_persona_id_for_access(
        &self,
        capabilities: &SessionSandboxCapabilities,
    ) -> Option<&str> {
        self.personas
            .as_ref()
            .and_then(|personas| personas.default_persona_id_for_access(&capabilities.repo_cache))
    }

    fn context(&self) -> RuntimeContext {
        RuntimeContext {
            store: self.store.clone(),
            manager: self.sandbox_runtime.manager.clone(),
            sandbox_pipes: self.sandbox_pipes.clone(),
            execution_spans: self.execution_spans.clone(),
            stdout_owner_id: self.stdout_owner_id.clone(),
            stdout_owner_renewals: self.stdout_owner_renewals.clone(),
        }
    }

    pub async fn run_tool_host_call(
        &self,
        input: ToolHostCallInput,
    ) -> Result<ToolHostCallOutput, SessionRuntimeError> {
        let principal_id = input.principal_id.trim().to_owned();
        let tool_name = input.tool_name.trim().to_owned();
        let method = input.method.trim().to_owned();
        if principal_id.is_empty() {
            return Err(SessionRuntimeError::BadRequest(
                "tool host principal_id is required".to_owned(),
            ));
        }
        if tool_name.is_empty() {
            return Err(SessionRuntimeError::BadRequest(
                "tool host tool_name is required".to_owned(),
            ));
        }
        if method.is_empty() {
            return Err(SessionRuntimeError::BadRequest(
                "tool host method is required".to_owned(),
            ));
        }
        if input.timeout.is_zero() {
            return Err(SessionRuntimeError::BadRequest(
                "tool host timeout must be non-zero".to_owned(),
            ));
        }

        let thread_key = tool_host_thread_key(&principal_id)?;
        let input = ToolHostCallInput {
            principal_id,
            tool_name,
            method,
            ..input
        };
        let call_lock = self.tool_host_call_lock(&thread_key);
        let result = {
            let _call_guard = call_lock.lock().await;
            self.locked_tool_host_call(&thread_key, input).await
        };
        // Drop our clone so an idle entry is only referenced by the map, then
        // evict it; remove_if holds the shard lock, so no concurrent caller
        // can clone the entry between the count check and the removal.
        drop(call_lock);
        self.tool_host_call_locks
            .remove_if(thread_key.as_str(), |_, lock| Arc::strong_count(lock) == 1);
        result
    }

    fn tool_host_call_lock(&self, thread_key: &ThreadKey) -> Arc<Mutex<()>> {
        self.tool_host_call_locks
            .entry(thread_key.as_str().to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn locked_tool_host_call(
        &self,
        thread_key: &ThreadKey,
        input: ToolHostCallInput,
    ) -> Result<ToolHostCallOutput, SessionRuntimeError> {
        let ToolHostCallInput {
            principal_id,
            token_id,
            tool_name,
            method,
            arguments,
            timeout,
        } = input;
        self.create_or_get_tool_host_session(thread_key, &principal_id)
            .await?;

        let request_id = format!("mcp-call-{}", Uuid::new_v4().simple());
        let request = ToolHostRequest {
            id: request_id.clone(),
            tool: tool_name.clone(),
            method: method.clone(),
            arguments,
            principal_id,
            token_id,
            timeout_seconds: timeout.as_secs().max(1),
        };
        let input_line = serde_json::to_string(&request).map_err(|error| {
            SessionRuntimeError::Sandbox(SandboxError::io_source("encode tool host request", error))
        })?;
        let response_timeout = timeout.saturating_add(Duration::from_secs(5));
        let execution = self
            .execute_session(
                thread_key,
                ExecuteSessionInput {
                    idempotency_key: Some(request_id.clone()),
                    metadata: Some(json!({
                        "mcp_tool_host_call": true,
                        "request_id": request_id,
                        "tool": tool_name,
                        "method": method,
                        "timeout_ms": duration_millis_u64(timeout),
                    })),
                    input_lines: vec![input_line],
                    idle_timeout_ms: None,
                    max_duration_ms: Some(duration_millis_u64(response_timeout)),
                },
            )
            .await?;
        self.wait_for_tool_host_call(thread_key, &execution.execution_id, response_timeout)
            .await
    }

    async fn create_or_get_tool_host_session(
        &self,
        thread_key: &ThreadKey,
        principal_id: &str,
    ) -> Result<(), SessionRuntimeError> {
        let harness = self
            .sandbox_runtime
            .warm_harness
            .clone()
            .unwrap_or(HarnessType::Codex);
        let metadata = tool_host_session_metadata(principal_id);
        let session = self
            .store
            .create_or_get_session(thread_key, &harness, None, metadata, BTreeMap::new())
            .await?;
        if self.iron_control.is_some()
            && session.iron_control_principal.as_deref() != Some(principal_id)
        {
            self.store
                .set_iron_control_principal(thread_key, Some(principal_id))
                .await?;
        }
        Ok(())
    }

    async fn wait_for_tool_host_call(
        &self,
        thread_key: &ThreadKey,
        execution_id: &str,
        response_timeout: Duration,
    ) -> Result<ToolHostCallOutput, SessionRuntimeError> {
        let events = self
            .stream_events(thread_key, 0, Some(execution_id))
            .await?;
        futures_util::pin_mut!(events);
        match timeout(response_timeout, async {
            while let Some(event) = events.next().await {
                let event = event?;
                match event.event_type.as_str() {
                    "session.execution_completed" => {
                        return self.tool_host_completed_output(thread_key, &event).await;
                    }
                    "session.execution_failed" => {
                        return self.tool_host_failed_output(thread_key, &event).await;
                    }
                    _ => {}
                }
            }
            Err(SessionRuntimeError::Sandbox(SandboxError::io(
                "session event stream ended before tool host call completed",
            )))
        })
        .await
        {
            Ok(output) => output,
            // Best-effort sandbox id: a store error must not replace the
            // timeout result with an internal error.
            Err(_) => Ok(ToolHostCallOutput {
                sandbox_id: self
                    .current_sandbox_id(thread_key)
                    .await
                    .unwrap_or_default(),
                stdout: String::new(),
                stderr: format!(
                    "tool host call timed out after {} ms",
                    response_timeout.as_millis()
                ),
                exit_status: None,
                timed_out: true,
            }),
        }
    }

    async fn tool_host_completed_output(
        &self,
        thread_key: &ThreadKey,
        event: &SessionEvent,
    ) -> Result<ToolHostCallOutput, SessionRuntimeError> {
        let sandbox_id = self.current_sandbox_id(thread_key).await?;
        let Some(result_text) = event.payload.get("result_text").and_then(Value::as_str) else {
            return Ok(ToolHostCallOutput {
                sandbox_id,
                stdout: String::new(),
                stderr: String::new(),
                exit_status: Some(0),
                timed_out: false,
            });
        };
        let response = serde_json::from_str::<ToolHostResponse>(result_text).map_err(|error| {
            SessionRuntimeError::Sandbox(SandboxError::io_source(
                "decode tool host response",
                error,
            ))
        })?;
        Ok(ToolHostCallOutput {
            sandbox_id,
            stdout: response.stdout,
            stderr: response.stderr,
            exit_status: response.status,
            timed_out: response.timed_out,
        })
    }

    async fn tool_host_failed_output(
        &self,
        thread_key: &ThreadKey,
        event: &SessionEvent,
    ) -> Result<ToolHostCallOutput, SessionRuntimeError> {
        let error = event
            .payload
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("tool host execution failed")
            .to_owned();
        let timed_out = event
            .payload
            .get("reason")
            .and_then(Value::as_str)
            .is_some_and(|reason| reason == "max_duration_exceeded");
        Ok(ToolHostCallOutput {
            sandbox_id: self.current_sandbox_id(thread_key).await?,
            stdout: String::new(),
            stderr: error,
            exit_status: None,
            timed_out,
        })
    }

    async fn current_sandbox_id(
        &self,
        thread_key: &ThreadKey,
    ) -> Result<String, SessionRuntimeError> {
        Ok(self
            .store
            .get_session(thread_key)
            .await?
            .sandbox_id
            .unwrap_or_default())
    }

    #[cfg(test)]
    async fn claim_stdout_owner(&self, execution_id: &str) -> Result<(), SessionRuntimeError> {
        let _claim_guard = self.stdout_owner_claim_gate.lock().await;
        if self.shutting_down.load(Ordering::SeqCst) {
            return Err(SessionRuntimeError::ShuttingDown);
        }
        if !stop_stdout_owner_renewer(&self.stdout_owner_renewals, execution_id).await {
            return Err(SessionRuntimeError::StdoutOwnerRenewerStopTimeout {
                execution_id: execution_id.to_owned(),
            });
        }
        self.stdout_owner_claim_db_started.notify_one();
        let claimed = self
            .store
            .claim_stdout_owner(execution_id, &self.stdout_owner_id, STDOUT_OWNER_LEASE)
            .await?;
        if !claimed {
            if self.shutting_down.load(Ordering::SeqCst) {
                return Err(SessionRuntimeError::ShuttingDown);
            }
            return Err(SessionRuntimeError::BadRequest(format!(
                "execution {execution_id} stdout is owned by another control plane process"
            )));
        }
        if self.shutting_down.load(Ordering::SeqCst) {
            self.abandon_stdout_owner(execution_id).await;
            return Err(SessionRuntimeError::ShuttingDown);
        }
        spawn_stdout_owner_renewer(self.context(), execution_id.to_owned());
        Ok(())
    }

    async fn claim_expired_stdout_owner(
        &self,
        execution_id: &str,
    ) -> Result<bool, SessionRuntimeError> {
        let _claim_guard = self.stdout_owner_claim_gate.lock().await;
        if self.shutting_down.load(Ordering::SeqCst) {
            return Err(SessionRuntimeError::ShuttingDown);
        }
        if !stop_stdout_owner_renewer(&self.stdout_owner_renewals, execution_id).await {
            return Err(SessionRuntimeError::StdoutOwnerRenewerStopTimeout {
                execution_id: execution_id.to_owned(),
            });
        }
        #[cfg(test)]
        self.stdout_owner_claim_db_started.notify_one();
        let claimed = self
            .store
            .claim_expired_stdout_owner(execution_id, &self.stdout_owner_id, STDOUT_OWNER_LEASE)
            .await?;
        if self.shutting_down.load(Ordering::SeqCst) {
            if claimed {
                self.abandon_stdout_owner(execution_id).await;
            }
            return Err(SessionRuntimeError::ShuttingDown);
        }
        if claimed {
            spawn_stdout_owner_renewer(self.context(), execution_id.to_owned());
        }
        Ok(claimed)
    }

    async fn claim_input_delivery(
        &self,
        execution_id: Option<&str>,
        delivery_id: Option<&str>,
    ) -> Result<Option<ClaimedInputDelivery>, SessionRuntimeError> {
        let _claim_guard = self.stdout_owner_claim_gate.lock().await;
        if self.shutting_down.load(Ordering::SeqCst) {
            return Err(SessionRuntimeError::ShuttingDown);
        }
        let claim = self
            .store
            .claim_next_input_delivery(
                &self.stdout_owner_id,
                STDOUT_OWNER_LEASE,
                execution_id,
                delivery_id,
            )
            .await?;
        if self.shutting_down.load(Ordering::SeqCst) {
            if let Some(claim) = &claim {
                self.abandon_stdout_owner(&claim.execution.execution_id)
                    .await;
            }
            return Err(SessionRuntimeError::ShuttingDown);
        }
        if let Some(claim) = &claim {
            if !stop_stdout_owner_renewer(
                &self.stdout_owner_renewals,
                &claim.execution.execution_id,
            )
            .await
            {
                self.abandon_stdout_owner(&claim.execution.execution_id)
                    .await;
                return Err(SessionRuntimeError::StdoutOwnerRenewerStopTimeout {
                    execution_id: claim.execution.execution_id.clone(),
                });
            }
            spawn_stdout_owner_renewer(self.context(), claim.execution.execution_id.clone());
        }
        Ok(claim)
    }

    async fn drive_claimed_input_delivery(
        &self,
        claim: &ClaimedInputDelivery,
    ) -> Result<(), SessionRuntimeError> {
        let thread_key = &claim.execution.thread_key;
        let execution_id = claim.execution.execution_id.as_str();
        let mut session = self.store.get_session(thread_key).await?;
        let capabilities = self
            .resolve_sandbox_capabilities(
                thread_key,
                &session.harness_type,
                session.iron_control_principal.as_deref(),
                Some(&claim.execution.metadata),
            )
            .await?;
        let boundary_fingerprint = input_delivery_boundary_fingerprint(
            thread_key,
            Some(&claim.execution.metadata),
            &capabilities,
        );
        if boundary_fingerprint != claim.delivery.boundary_fingerprint {
            if let Some(sandbox_id) = session.sandbox_id.as_deref() {
                self.discard_sandbox_before_input(thread_key, sandbox_id)
                    .await?;
            }
            if !self
                .store
                .rebind_claimed_input_delivery_boundary(
                    &claim.delivery.delivery_id,
                    &self.stdout_owner_id,
                    claim.delivery.owner_generation,
                    &boundary_fingerprint,
                    metadata_trace_execution_boundary(&capabilities),
                )
                .await?
            {
                return Err(SessionRuntimeError::MetadataTraceBoundaryChanged);
            }
            session = self.store.get_session(thread_key).await?;
        }
        let sandbox_id = self
            .ensure_session_sandbox(EnsureSessionSandboxRequest {
                thread_key,
                harness_type: &session.harness_type,
                persona_id: session.persona_id.as_deref(),
                existing_sandbox_id: session.sandbox_id.as_deref(),
                existing_sandbox_capabilities: session.sandbox_capabilities.as_ref(),
                iron_control_principal: session.iron_control_principal.as_deref(),
                proxy_labels: &session.proxy_labels,
                desired_capabilities: &capabilities,
                execution_metadata: Some(&claim.execution.metadata),
                execution_id,
            })
            .await?;
        let pipe = self
            .ensure_session_pipe_with_output_state(
                thread_key,
                &sandbox_id,
                stdout_state_for_execution(&session, execution_id),
            )
            .await?;
        self.flush_claimed_input_delivery(
            &pipe,
            claim,
            &sandbox_id,
            &capabilities,
            &boundary_fingerprint,
        )
        .await
    }

    async fn finish_failed_input_delivery(
        &self,
        claim: &ClaimedInputDelivery,
        error: &SessionRuntimeError,
    ) {
        let result = self
            .store
            .mark_input_delivery_ambiguous(
                &claim.delivery.delivery_id,
                &self.stdout_owner_id,
                claim.delivery.owner_generation,
                &error.to_string(),
            )
            .await;
        if let Err(store_error) = result {
            warn!(
                delivery_id = %claim.delivery.delivery_id,
                execution_id = %claim.execution.execution_id,
                %store_error,
                "failed to persist input-delivery failure disposition"
            );
        }
    }

    async fn flush_claimed_input_delivery(
        &self,
        pipe: &SessionPipe,
        claim: &ClaimedInputDelivery,
        sandbox_id: &str,
        capabilities: &SessionSandboxCapabilities,
        boundary_fingerprint: &str,
    ) -> Result<(), SessionRuntimeError> {
        let assignment = SandboxAssignmentIdentity {
            assignment_epoch: pipe
                .assignment_epoch
                .clone()
                .ok_or(SessionRuntimeError::SandboxAssignmentChanged)?,
            resource_uid: pipe.resource_uid.clone(),
        };
        let mut stdin = pipe.stdin.lock().await;
        let Some(guard) = self
            .store
            .begin_input_delivery_flush(
                &claim.delivery.delivery_id,
                &self.stdout_owner_id,
                claim.delivery.owner_generation,
                sandbox_id,
                capabilities,
                boundary_fingerprint,
                &assignment,
            )
            .await?
        else {
            return Err(SessionRuntimeError::MetadataTraceBoundaryChanged);
        };
        let write_timeout = guard
            .remaining()
            .map(|remaining| remaining.min(METADATA_TRACE_INPUT_WRITE_MAX))
            .ok_or(SessionRuntimeError::MetadataTraceBoundaryChanged)?;
        let write = async {
            for line in &claim.delivery.input_lines {
                stdin.send(line).await.map_err(codec_error_to_runtime)?;
            }
            io::AsyncWriteExt::flush(stdin.get_mut())
                .await
                .map_err(|error| {
                    SessionRuntimeError::Sandbox(SandboxError::io_source("flush stdin", error))
                })
        };
        if let Err(error) = timeout(write_timeout, write)
            .await
            .map_err(|_| SessionRuntimeError::MetadataTraceBoundaryChanged)
            .and_then(|result| result)
        {
            let _ = guard.rollback().await;
            let _ = self
                .store
                .mark_input_delivery_ambiguous(
                    &claim.delivery.delivery_id,
                    &self.stdout_owner_id,
                    claim.delivery.owner_generation,
                    &error.to_string(),
                )
                .await;
            return Err(error);
        }
        let committed = guard.commit().await?;
        if committed.is_none() {
            let _ = self
                .store
                .mark_input_delivery_ambiguous(
                    &claim.delivery.delivery_id,
                    &self.stdout_owner_id,
                    claim.delivery.owner_generation,
                    "flush completed but durable commit lost its owner fence",
                )
                .await;
            return Err(SessionRuntimeError::MetadataTraceBoundaryChanged);
        }
        Ok(())
    }

    async fn abandon_stdout_owner(&self, execution_id: &str) -> bool {
        if !stop_stdout_owner_renewer(&self.stdout_owner_renewals, execution_id).await {
            warn!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "session_stdout_owner_renewer_stop_timeout",
                execution_id,
                stdout_owner_id = %self.stdout_owner_id,
                "stdout owner renewer did not stop; leaving the lease fenced"
            );
            return false;
        }
        #[cfg(test)]
        self.stdout_owner_release_started.notify_one();
        match timeout(
            STDOUT_OWNER_RELEASE_TIMEOUT,
            self.store
                .release_stdout_owner(execution_id, &self.stdout_owner_id),
        )
        .await
        {
            Ok(Ok(released)) => released,
            Ok(Err(error)) => {
                warn!(
                    component = COMPONENT_SESSION_RUNTIME,
                    event = "session_stdout_owner_release_failed",
                    execution_id,
                    stdout_owner_id = %self.stdout_owner_id,
                    %error,
                    "failed to release abandoned stdout owner lease; it will expire naturally"
                );
                false
            }
            Err(_) => {
                warn!(
                    component = COMPONENT_SESSION_RUNTIME,
                    event = "session_stdout_owner_release_timeout",
                    execution_id,
                    stdout_owner_id = %self.stdout_owner_id,
                    timeout_ms = duration_millis_u64(STDOUT_OWNER_RELEASE_TIMEOUT),
                    "timed out releasing abandoned stdout owner lease; it will expire naturally"
                );
                false
            }
        }
    }

    /// Attach an iron-control registrar so each new session upserts its
    /// principal and assigns it the configured roles.
    pub fn with_iron_control(mut self, registrar: SessionRegistrar) -> Self {
        self.iron_control = Some(registrar);
        self
    }

    pub async fn slack_trace_consent(
        &self,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<MetadataTraceConsent, SessionRuntimeError> {
        Ok(self
            .store
            .metadata_trace_consent("slack", workspace_id, user_id)
            .await?)
    }

    pub async fn set_slack_trace_consent(
        &self,
        workspace_id: &str,
        user_id: &str,
        expires_at: OffsetDateTime,
        expected_revision: i64,
        idempotency: (&str, String),
    ) -> Result<MetadataTraceConsent, SessionRuntimeError> {
        Ok(self
            .store
            .grant_metadata_trace_consent_idempotent(
                "slack",
                workspace_id,
                user_id,
                expires_at,
                Some(expected_revision),
                idempotency.0,
                &idempotency.1,
            )
            .await?)
    }

    pub async fn revoke_slack_trace_consent(
        &self,
        workspace_id: &str,
        user_id: &str,
        idempotency: Option<(&str, String)>,
    ) -> Result<MetadataTraceConsent, SessionRuntimeError> {
        // The commit is the acknowledgement fence: it disables future traced
        // input and records the exact assignments that a reconciler must
        // retire. Backend shutdown is deliberately not on the HTTP path.
        let (consent, _) = self
            .revoke_slack_trace_consent_durable(workspace_id, user_id, idempotency)
            .await?;
        Ok(consent)
    }

    async fn revoke_slack_trace_consent_durable(
        &self,
        workspace_id: &str,
        user_id: &str,
        idempotency: Option<(&str, String)>,
    ) -> Result<(MetadataTraceConsent, Vec<MetadataTraceDrainTarget>), SessionRuntimeError> {
        let subject = SlackTraceSubject::from_parts(workspace_id, user_id);
        let subject_hash = trace_subject_hash(&subject);
        Ok(match idempotency {
            Some((key, request_hash)) => {
                self.store
                    .revoke_metadata_trace_consent_idempotent(
                        "slack",
                        workspace_id,
                        user_id,
                        &subject_hash,
                        key,
                        &request_hash,
                    )
                    .await?
            }
            None => {
                self.store
                    .revoke_metadata_trace_consent("slack", workspace_id, user_id, &subject_hash)
                    .await?
            }
        })
    }

    /// Retire exact sandbox assignments after their revoke has committed. This
    /// worker may wait for backend shutdown; callers on the acknowledgement
    /// path must use [`Self::revoke_slack_trace_consent`] instead.
    async fn drain_slack_trace_consent(
        &self,
        pending: &MetadataTraceConsent,
    ) -> Result<(), SessionRuntimeError> {
        let workspace_id = &pending.workspace_id;
        let user_id = &pending.user_id;
        let Some(targets) = self
            .store
            .metadata_trace_drain_targets_if_current(
                "slack",
                workspace_id,
                user_id,
                pending.revision,
            )
            .await?
        else {
            // Another replica already completed this revision and may have
            // regranted consent. Never turn stale drain work into a revoke.
            return Ok(());
        };
        for target in targets {
            let Some(assignment_lock) = self
                .store
                .lock_sandbox_assignment_for_reconciliation(&target.thread_key, &target.sandbox_id)
                .await?
            else {
                continue;
            };
            if assignment_lock.resource_uid() != Some(target.resource_uid.as_str())
                || assignment_lock.metadata_trace_assignment_epoch()
                    != Some(target.assignment_epoch.as_str())
            {
                assignment_lock.rollback().await?;
                continue;
            }
            let sandbox_id = SandboxId::new(&target.sandbox_id);
            match observe_assignment_reconciliation(&self.sandbox_runtime.manager, &sandbox_id)
                .await
            {
                Ok(observation)
                    if (observation.status == SandboxStatus::Gone
                        && observation.resource_uid.is_none())
                        || observation
                            .resource_uid
                            .as_deref()
                            .is_some_and(|resource_uid| resource_uid != target.resource_uid) =>
                {
                    // A missing optional UID is not proof that the exact
                    // sandbox disappeared. Only Gone or a concrete different
                    // UID can retire the durable target without a stop.
                    if !assignment_lock.commit_if_current().await? {
                        continue;
                    }
                    self.sandbox_pipes
                        .remove_if(&target.sandbox_id, |_id, pipe| {
                            pipe.trace_resource_uid.as_deref() == Some(target.resource_uid.as_str())
                                && pipe.trace_assignment_epoch.as_deref()
                                    == Some(target.assignment_epoch.as_str())
                        });
                    self.store
                        .clear_metadata_trace_assignment_if_matches(
                            &target,
                            "slack",
                            workspace_id,
                            user_id,
                        )
                        .await?;
                    continue;
                }
                Err(SandboxError::NotFound(_)) => {
                    if !assignment_lock.commit_if_current().await? {
                        continue;
                    }
                    self.sandbox_pipes
                        .remove_if(&target.sandbox_id, |_id, pipe| {
                            pipe.trace_resource_uid.as_deref() == Some(target.resource_uid.as_str())
                                && pipe.trace_assignment_epoch.as_deref()
                                    == Some(target.assignment_epoch.as_str())
                        });
                    self.store
                        .clear_metadata_trace_assignment_if_matches(
                            &target,
                            "slack",
                            workspace_id,
                            user_id,
                        )
                        .await?;
                    continue;
                }
                Ok(_) => {}
                Err(error) => {
                    assignment_lock.rollback().await?;
                    warn!(sandbox_id = %target.sandbox_id, %error, "metadata trace sandbox drain observation failed");
                    continue;
                }
            }
            let drained = match stop_exact_and_confirm(
                &self.sandbox_runtime.manager,
                &target.sandbox_id,
                &target.resource_uid,
            )
            .await
            {
                Ok((drained, _)) => drained,
                Err(error) => {
                    warn!(sandbox_id = %target.sandbox_id, %error, "metadata trace sandbox drain stop failed");
                    false
                }
            };
            if drained {
                if !assignment_lock.commit_if_current().await? {
                    continue;
                }
                self.sandbox_pipes
                    .remove_if(&target.sandbox_id, |_id, pipe| {
                        pipe.trace_resource_uid.as_deref() == Some(target.resource_uid.as_str())
                            && pipe.trace_assignment_epoch.as_deref()
                                == Some(target.assignment_epoch.as_str())
                    });
                self.store
                    .clear_metadata_trace_assignment_if_matches(
                        &target,
                        "slack",
                        workspace_id,
                        user_id,
                    )
                    .await?;
            } else {
                assignment_lock.rollback().await?;
                // The already-persisted disabled + drain_pending result is the
                // only safe acknowledgement. The reconciler retries this
                // exact epoch instead of claiming the sidecar is gone.
                warn!(sandbox_id = %target.sandbox_id, "metadata trace sandbox drain remains pending");
            }
        }
        if pending.drain_pending {
            // The request's durable result deliberately remains pending: a
            // replay must return the exact accepted result while it resumes
            // drain work, rather than exposing a later mutation of that
            // result. The consent row is still cleared once exact targets are
            // gone, so future grants are not blocked.
            let _ = self
                .store
                .complete_metadata_trace_drain_if_current_and_empty(
                    "slack",
                    workspace_id,
                    user_id,
                    pending.revision,
                )
                .await?;
        }
        Ok(())
    }

    pub fn with_metadata_trace_config(
        mut self,
        config: Option<MetadataTraceConfigIdentity>,
    ) -> Self {
        self.metadata_trace_config = config;
        self
    }

    /// Register the shared unauthenticated MCP tool-host principal when
    /// iron-control is enabled, so proxy-backed tool calls can resolve an
    /// effective config without minting per-user credentials in this layer.
    pub async fn register_mcp_tool_host_principal(
        &self,
        principal_id: &str,
    ) -> Result<String, SessionRuntimeError> {
        let principal_id = principal_id.trim();
        if principal_id.is_empty() {
            return Err(SessionRuntimeError::BadRequest(
                "mcp tool host principal_id is required".to_owned(),
            ));
        }
        if principal_id.contains(':') {
            return Err(SessionRuntimeError::BadRequest(
                "mcp tool host principal_id must not contain ':'".to_owned(),
            ));
        }
        let thread_key = tool_host_thread_key(principal_id)?;
        if let Some(registrar) = &self.iron_control {
            // Serialize with run_tool_host_call so concurrent registrations
            // for the same principal cannot interleave with session setup.
            let call_lock = self.tool_host_call_lock(&thread_key);
            let _call_guard = call_lock.lock().await;
            let metadata = tool_host_session_metadata(principal_id);
            let principal = registrar
                .register_session(thread_key.as_str(), Some(&metadata))
                .await?;
            return Ok(principal.id);
        }
        Ok(principal_id.to_owned())
    }

    pub fn with_warm_pool(mut self, config: WarmPoolConfig) -> Self {
        if config.target_size == 0 {
            return self;
        }

        let (Some(spec_factory), Some(workload_key)) = (
            self.sandbox_runtime.warm_spec_factory.clone(),
            self.sandbox_runtime.workload_key.clone(),
        ) else {
            warn!(
                target_size = config.target_size,
                "session sandbox warm pool requested for runtime without a warm sandbox spec"
            );
            return self;
        };

        let pool = Arc::new(WarmPoolManager::new(
            self.sandbox_runtime.manager.clone(),
            self.store.clone(),
            spec_factory,
            workload_key,
            config,
        ));
        pool.clone().spawn_replenisher();
        self.warm_pool = Some(pool);
        self
    }

    pub fn with_sandbox_capacity(mut self, config: SandboxCapacityConfig) -> Self {
        if !config.is_enabled() {
            return self;
        }
        self.capacity = Some(Arc::new(SandboxCapacityController::new(
            self.store.clone(),
            self.sandbox_runtime.manager.clone(),
            self.sandbox_pipes.clone(),
            config,
        )));
        self
    }

    async fn run_with_running_capacity<T, F, Fut>(
        &self,
        thread_key: &ThreadKey,
        execution_id: &str,
        operation: &'static str,
        action: F,
    ) -> Result<T, SessionRuntimeError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, SessionRuntimeError>>,
    {
        if let Some(capacity) = self.capacity.as_ref() {
            capacity
                .run_with_capacity(thread_key, execution_id, operation, action)
                .await
        } else {
            action().await
        }
    }

    async fn run_reuse_fence_with_capacity<T, F, Fut>(
        &self,
        sandbox_id: &SandboxId,
        thread_key: &ThreadKey,
        execution_id: &str,
        action: F,
    ) -> Result<T, SessionRuntimeError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, SessionRuntimeError>>,
    {
        if let Some(capacity) = self.capacity.as_ref() {
            capacity
                .run_reuse_fence_with_capacity(sandbox_id, thread_key, execution_id, action)
                .await
        } else {
            action().await
        }
    }

    /// Spawn the background reaper that stops expired sandboxes and always
    /// resumes finalizer-retained cleanup, even without a max lifetime.
    pub fn with_sandbox_reaper(self, config: SandboxReaperConfig) -> Self {
        if !config.is_enabled() {
            return self;
        }
        SandboxReaper::new(self.sandbox_runtime.manager.clone(), config).spawn();
        self
    }

    /// Spawn the DB-aware cleanup worker that reaps backend sandboxes no durable
    /// session/warm-pool row references and restores idle pauses lost across
    /// control-plane restarts.
    pub fn with_sandbox_cleanup(self, config: SessionSandboxCleanupConfig) -> Self {
        if !config.is_enabled() {
            return self;
        }
        cleanup::SessionSandboxCleanupWorker::new(self.context(), config).spawn();
        self
    }

    /// Reconcile already-running principal sandboxes independently of a new
    /// execution. Revocation and changed deployment trace config therefore
    /// stop the old capability boundary promptly.
    pub fn with_sandbox_capability_reconciler(self) -> Self {
        let runtime = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                if let Err(error) = runtime.reconcile_active_sandbox_capabilities().await {
                    warn!(%error, "active sandbox capability reconciliation failed");
                }
            }
        });
        self
    }

    pub async fn reconcile_active_sandbox_capabilities(
        &self,
    ) -> Result<usize, SessionRuntimeError> {
        let mut stopped = 0;
        self.store.expire_elapsed_metadata_trace_consents().await?;
        for pending in self.store.pending_metadata_trace_drains().await? {
            if pending.source == "slack" {
                self.drain_slack_trace_consent(&pending).await?;
                stopped += 1;
            }
        }
        let Some(identity) = self.metadata_trace_config.as_ref() else {
            // A no-trace deployment must still retire a sandbox carrying an
            // old traced assignment. There is no trace generation lease to
            // acquire in this legacy/no-generation mode; conditional clears
            // remain the durable ownership fence.
            return self
                .reconcile_active_sandbox_capabilities_without_trace_config()
                .await
                .map(|count| count + stopped);
        };
        let Some(lease) = self
            .store
            .acquire_metadata_trace_reconciler_lease(
                identity,
                &self.metadata_trace_reconciler_owner_id,
                METADATA_TRACE_RECONCILER_LEASE,
            )
            .await?
        else {
            return Ok(stopped);
        };
        self.reconcile_active_sandbox_capabilities_with_lease(identity, &lease)
            .await
            .map(|count| count + stopped)
    }

    async fn reconcile_active_sandbox_capabilities_without_trace_config(
        &self,
    ) -> Result<usize, SessionRuntimeError> {
        let sessions = self.store.list_principal_sandbox_sessions().await?;
        let mut stopped = 0;
        for session in sessions {
            // Without a configured trace identity, this pass owns only the
            // stale trace boundary. Principal capability reconciliation needs
            // an active registrar/configuration source.
            if session.sandbox_id.is_none()
                || !session
                    .sandbox_capabilities
                    .as_ref()
                    .is_some_and(|capabilities| capabilities.metadata_trace_enabled)
            {
                continue;
            }
            if !self.persisted_trace_boundary_is_current(&session).await? {
                match self
                    .reconcile_session_sandbox_capabilities(
                        &session,
                        SessionSandboxCapabilities::default_enabled(),
                    )
                    .await
                {
                    Ok(true) => stopped += 1,
                    Ok(false) => {}
                    Err(error) => warn!(
                        thread_key = %session.thread_key,
                        sandbox_id = session.sandbox_id.as_deref().unwrap_or(""),
                        %error,
                        "stale trace sandbox reconciliation remains retryable"
                    ),
                }
                continue;
            }
            let outcome = match self
                .resolve_sandbox_capabilities(
                    &session.thread_key,
                    &session.harness_type,
                    session.iron_control_principal.as_deref(),
                    self.metadata_trace_assignment_metadata(&session)
                        .await?
                        .as_ref(),
                )
                .await
            {
                Ok(desired)
                    if sandbox_capabilities_match(
                        session.sandbox_capabilities.as_ref(),
                        &desired,
                    ) =>
                {
                    Ok(false)
                }
                Ok(desired) => {
                    self.reconcile_session_sandbox_capabilities(&session, desired)
                        .await
                }
                Err(SessionRuntimeError::IronControl(error))
                    if is_deleted_principal_error(&error)
                        || session_has_expired_metadata_trace_consent(
                            &session,
                            OffsetDateTime::now_utc(),
                        ) =>
                {
                    self.reconcile_session_sandbox_capabilities(
                        &session,
                        SessionSandboxCapabilities::default_enabled(),
                    )
                    .await
                }
                Err(error) => Err(error),
            };
            match outcome {
                Ok(true) => stopped += 1,
                Ok(false) => {}
                Err(error) => warn!(
                    thread_key = %session.thread_key,
                    sandbox_id = session.sandbox_id.as_deref().unwrap_or(""),
                    %error,
                    "active sandbox capability reconciliation failed for session"
                ),
            }
        }
        Ok(stopped)
    }

    async fn reconcile_active_sandbox_capabilities_with_lease(
        &self,
        identity: &MetadataTraceConfigIdentity,
        lease: &MetadataTraceReconcilerLease,
    ) -> Result<usize, SessionRuntimeError> {
        let sessions = self.store.list_principal_sandbox_sessions().await?;
        let mut stopped = 0;
        for session in sessions {
            if !self
                .store
                .metadata_trace_reconciler_lease_is_active(identity, lease)
                .await?
            {
                return Ok(stopped);
            }
            if session.sandbox_id.is_none() {
                continue;
            }
            if !self.persisted_trace_boundary_is_current(&session).await? {
                match self
                    .reconcile_session_sandbox_capabilities(
                        &session,
                        SessionSandboxCapabilities::default_enabled(),
                    )
                    .await
                {
                    Ok(true) => stopped += 1,
                    Ok(false) => {}
                    Err(error) => warn!(
                        thread_key = %session.thread_key,
                        sandbox_id = session.sandbox_id.as_deref().unwrap_or(""),
                        %error,
                        "stale trace sandbox reconciliation remains retryable"
                    ),
                }
                continue;
            }
            let outcome = match self
                .resolve_sandbox_capabilities(
                    &session.thread_key,
                    &session.harness_type,
                    session.iron_control_principal.as_deref(),
                    self.metadata_trace_assignment_metadata(&session)
                        .await?
                        .as_ref(),
                )
                .await
            {
                Ok(desired)
                    if sandbox_capabilities_match(
                        session.sandbox_capabilities.as_ref(),
                        &desired,
                    ) =>
                {
                    Ok(false)
                }
                Ok(desired) => {
                    self.reconcile_session_sandbox_capabilities(&session, desired)
                        .await
                }
                Err(SessionRuntimeError::IronControl(error))
                    if is_deleted_principal_error(&error)
                        || session_has_expired_metadata_trace_consent(
                            &session,
                            OffsetDateTime::now_utc(),
                        ) =>
                {
                    // A missing principal revokes immediately. For transient
                    // control-plane failures, the persisted expiry remains the
                    // hard stop: an outage must never extend a trace lease.
                    self.reconcile_session_sandbox_capabilities(
                        &session,
                        SessionSandboxCapabilities::default_enabled(),
                    )
                    .await
                }
                Err(error) => Err(error),
            };

            if !self
                .store
                .metadata_trace_reconciler_lease_is_active(identity, lease)
                .await?
            {
                return Ok(stopped);
            }
            match outcome {
                Ok(true) => stopped += 1,
                Ok(false) => {}
                Err(error) => warn!(
                    thread_key = %session.thread_key,
                    sandbox_id = session.sandbox_id.as_deref().unwrap_or(""),
                    %error,
                    "active sandbox capability reconciliation failed for session"
                ),
            }
        }
        Ok(stopped)
    }

    async fn reconcile_session_sandbox_capabilities(
        &self,
        session: &Session,
        desired: SessionSandboxCapabilities,
    ) -> Result<bool, SessionRuntimeError> {
        let Some(sandbox_id) = session.sandbox_id.as_deref() else {
            return Ok(false);
        };

        // Lock the current assignment before touching the backend. This makes
        // a replacement linearizable with the stop: a stale sweep observes the
        // replacement and does nothing, or a replacement waits until the old
        // sandbox has been retired and cleared.
        let Some(mut assignment_lock) = self
            .store
            .lock_sandbox_assignment_for_reconciliation(&session.thread_key, sandbox_id)
            .await?
        else {
            return Ok(false);
        };

        if assignment_lock.resource_uid().is_none() || assignment_lock.assignment_epoch().is_none()
        {
            let previous_resource_uid = assignment_lock.resource_uid().map(str::to_owned);
            let previous_assignment_epoch = assignment_lock.assignment_epoch().map(str::to_owned);
            let id = SandboxId::new(sandbox_id);
            let observed_uid =
                match observe_assignment_reconciliation(&self.sandbox_runtime.manager, &id).await {
                    Ok(observation)
                        if observation.status == SandboxStatus::Gone
                            && observation.resource_uid.is_none() =>
                    {
                        None
                    }
                    Ok(observation) => observation.resource_uid,
                    Err(SandboxError::NotFound(_)) => None,
                    Err(error) => {
                        assignment_lock.rollback().await?;
                        return Err(SessionRuntimeError::Sandbox(error));
                    }
                };
            let Some(observed_uid) = observed_uid else {
                let cleared = assignment_lock.clear_and_commit().await?;
                if cleared {
                    self.sandbox_pipes.remove_if(sandbox_id, |_id, pipe| {
                        pipe.resource_uid.as_deref() == previous_resource_uid.as_deref()
                            && pipe.assignment_epoch.as_deref()
                                == previous_assignment_epoch.as_deref()
                    });
                }
                self.store
                    .append_event(
                        &session.thread_key,
                        None,
                        "session.sandbox_capabilities_reconciled",
                        json!({
                            "sandbox_id": sandbox_id,
                            "previous_capabilities": session.sandbox_capabilities,
                            "desired_capabilities": desired,
                            "legacy_assignment_missing": true,
                        }),
                    )
                    .await?;
                return Ok(cleared);
            };
            if !assignment_lock
                .initialize_legacy_identity(&observed_uid)
                .await?
            {
                assignment_lock.rollback().await?;
                return Err(SessionRuntimeError::SandboxAssignmentChanged);
            }
        }

        let (Some(resource_uid), Some(assignment_epoch)) = (
            assignment_lock.resource_uid().map(str::to_owned),
            assignment_lock.assignment_epoch().map(str::to_owned),
        ) else {
            assignment_lock.rollback().await?;
            return Err(SessionRuntimeError::SandboxAssignmentChanged);
        };
        if !stop_exact_and_confirm(&self.sandbox_runtime.manager, sandbox_id, &resource_uid)
            .await?
            .0
        {
            // A successful delete request is not proof that the exact
            // backend resource is gone. Keep the durable assignment until a
            // later reconciler observes name/UID disappearance.
            assignment_lock.rollback().await?;
            return Ok(false);
        }
        // Keep the durable assignment until the stop has succeeded. A failed
        // backend delete rolls back the lock and remains visible to the next
        // reconciliation sweep, rather than orphaning a credentialed trace
        // pod.
        let cleared = assignment_lock.clear_and_commit().await?;
        if cleared {
            remove_pipe_for_assignment(
                &self.sandbox_pipes,
                sandbox_id,
                &resource_uid,
                &assignment_epoch,
            );
        }
        self.store
            .append_event(
                &session.thread_key,
                None,
                "session.sandbox_capabilities_reconciled",
                json!({
                    "sandbox_id": sandbox_id,
                    "previous_capabilities": session.sandbox_capabilities,
                    "desired_capabilities": desired,
                }),
            )
            .await?;
        Ok(cleared)
    }

    pub async fn create_or_get_session(
        &self,
        thread_key: &ThreadKey,
        harness_type: &HarnessType,
        persona_id: Option<&str>,
        metadata: Option<Value>,
        on_harness_conflict: HarnessConflictPolicy,
    ) -> Result<CreateOrGetSessionOutcome, SessionRuntimeError> {
        self.create_or_get_session_inner(
            thread_key,
            harness_type,
            persona_id,
            metadata,
            on_harness_conflict,
            None,
        )
        .await
    }

    pub async fn create_or_get_session_bound_to_principal(
        &self,
        thread_key: &ThreadKey,
        harness_type: &HarnessType,
        persona_id: Option<&str>,
        metadata: Option<Value>,
        on_harness_conflict: HarnessConflictPolicy,
        principal_id: &str,
    ) -> Result<CreateOrGetSessionOutcome, SessionRuntimeError> {
        let principal_id = principal_id.trim();
        if principal_id.is_empty() {
            return Err(SessionRuntimeError::BadRequest(
                "bound session principal_id is required".to_owned(),
            ));
        }
        self.create_or_get_session_inner(
            thread_key,
            harness_type,
            persona_id,
            metadata,
            on_harness_conflict,
            Some(principal_id),
        )
        .await
    }

    async fn create_or_get_session_inner(
        &self,
        thread_key: &ThreadKey,
        harness_type: &HarnessType,
        persona_id: Option<&str>,
        metadata: Option<Value>,
        on_harness_conflict: HarnessConflictPolicy,
        bound_principal_id: Option<&str>,
    ) -> Result<CreateOrGetSessionOutcome, SessionRuntimeError> {
        let span = info_span!(
            "centaur.api_rs.session.create_or_get",
            component = COMPONENT_SESSION_RUNTIME,
            event = "session_create_or_get",
            "centaur.thread_key" = thread_key.as_str(),
            "centaur.harness_type" = %harness_type,
            thread_key = %thread_key,
            harness_type = %harness_type,
            iron_control_enabled = self.iron_control.is_some(),
            bound_principal = bound_principal_id.is_some(),
        );
        set_span_parent_trace(
            &span,
            &thread_trace_id(thread_key),
            &thread_trace_parent_span_id(thread_key),
        );
        let result = async {
            ensure_thread_trace_root_span(thread_key);
            info!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "session_create_or_get_started",
                thread_key = %thread_key,
                harness_type = %harness_type,
                iron_control_enabled = self.iron_control.is_some(),
                bound_principal = bound_principal_id.is_some(),
                "creating or loading session"
            );
            let mut harness_switched = false;
            let mut session_metadata = default_metadata(metadata);
            let proxy_labels = proxy_labels_from_session_metadata(thread_key, &session_metadata);
            let (effective_principal, desired_capabilities) =
                match (&self.iron_control, bound_principal_id) {
                    (Some(registrar), Some(principal_id)) => {
                        let principal = registrar.get_principal(principal_id).await?;
                        let desired_capabilities = sandbox_capabilities_from_principal(&principal);
                        (Some(principal), desired_capabilities)
                    }
                    (Some(registrar), None) => {
                        let principal = registrar
                            .register_session(thread_key.as_str(), Some(&session_metadata))
                            .await?;
                        let desired_capabilities = sandbox_capabilities_from_principal(&principal);
                        (Some(principal), desired_capabilities)
                    }
                    (None, Some(_)) => {
                        return Err(SessionRuntimeError::BadRequest(
                            "bound session principals require Iron Control".to_owned(),
                        ));
                    }
                    (None, None) => (None, SessionSandboxCapabilities::default_enabled()),
                };
            let persona_resolution =
                self.resolve_persona_for_create(persona_id, &desired_capabilities)?;
            if let Some(context) = persona_resolution.context.as_ref() {
                add_persona_metadata(&mut session_metadata, context);
            }
            let principal_id = effective_principal
                .as_ref()
                .map(|principal| principal.id.as_str());
            let create_result = match (principal_id, bound_principal_id.is_some()) {
                (Some(principal_id), true) => {
                    self.store
                        .create_or_get_session_with_exact_principal(
                            thread_key,
                            harness_type,
                            persona_resolution.persona_id.as_deref(),
                            session_metadata.clone(),
                            proxy_labels.clone(),
                            principal_id,
                        )
                        .await
                }
                (Some(principal_id), false) => {
                    self.store
                        .create_or_get_session_with_principal(
                            thread_key,
                            harness_type,
                            persona_resolution.persona_id.as_deref(),
                            session_metadata.clone(),
                            proxy_labels.clone(),
                            principal_id,
                        )
                        .await
                }
                (None, _) => {
                    self.store
                        .create_or_get_session(
                            thread_key,
                            harness_type,
                            persona_resolution.persona_id.as_deref(),
                            session_metadata.clone(),
                            proxy_labels.clone(),
                        )
                        .await
                }
            };
            let session = match create_result {
                Ok(session) => session,
                Err(SessionStoreError::PersonaConflict { existing, .. })
                    if persona_id.is_none() && persona_resolution.defaulted =>
                {
                    match (principal_id, bound_principal_id.is_some()) {
                        (Some(principal_id), true) => {
                            self.store
                                .create_or_get_session_with_exact_principal(
                                    thread_key,
                                    harness_type,
                                    existing.as_deref(),
                                    default_metadata(None),
                                    BTreeMap::new(),
                                    principal_id,
                                )
                                .await?
                        }
                        (Some(principal_id), false) => {
                            self.store
                                .create_or_get_session_with_principal(
                                    thread_key,
                                    harness_type,
                                    existing.as_deref(),
                                    default_metadata(None),
                                    BTreeMap::new(),
                                    principal_id,
                                )
                                .await?
                        }
                        (None, _) => {
                            self.store
                                .create_or_get_session(
                                    thread_key,
                                    harness_type,
                                    existing.as_deref(),
                                    default_metadata(None),
                                    BTreeMap::new(),
                                )
                                .await?
                        }
                    }
                }
                Err(SessionStoreError::HarnessConflict { existing, .. })
                    if on_harness_conflict == HarnessConflictPolicy::Restart =>
                {
                    let session = self
                        .restart_session_on_harness(thread_key, harness_type, &existing)
                        .await?;
                    harness_switched = true;
                    session
                }
                Err(error) => return Err(error.into()),
            };
            if let Some(context) = self.resolve_stored_persona(
                session.persona_id.as_deref(),
                harness_type,
                &desired_capabilities,
            )? {
                self.store
                    .append_event(
                        thread_key,
                        None,
                        "session.persona_resolved",
                        json!({
                            "persona": context,
                            "requested_persona_id": persona_id,
                            "deployment_default_persona_id": self.default_persona_id(),
                        }),
                    )
                    .await?;
            }
            info!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "session_create_or_get_completed",
                thread_key = %thread_key,
                harness_type = %harness_type,
                status = %session.status,
                iron_control_principal_persisted = session.iron_control_principal.is_some(),
                harness_switched,
                "session ready"
            );
            Ok(CreateOrGetSessionOutcome {
                session,
                harness_switched,
            })
        }
        .instrument(span)
        .await;

        if let Err(error) = &result {
            error!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "session_create_or_get_failed",
                thread_key = %thread_key,
                harness_type = %harness_type,
                %error,
                "failed to create or load session"
            );
        }
        result
    }

    /// Restart an existing session on a different harness: stop its sandbox
    /// (killing any in-flight execution), clear the harness thread state, and
    /// flip the session row to the requested harness. Stored messages and
    /// events are preserved for the record, but the new harness boots with no
    /// conversational memory — callers that want continuity must re-send
    /// context with the next turn.
    async fn restart_session_on_harness(
        &self,
        thread_key: &ThreadKey,
        harness_type: &HarnessType,
        previous_harness: &str,
    ) -> Result<Session, SessionRuntimeError> {
        let previous = self.store.get_session(thread_key).await?;
        if let Some(sandbox_id) = previous.sandbox_id.as_deref() {
            let Some(assignment_lock) = self
                .store
                .lock_sandbox_assignment_for_reconciliation(thread_key, sandbox_id)
                .await?
            else {
                return Err(SessionRuntimeError::SandboxAssignmentChanged);
            };
            let (Some(resource_uid), Some(assignment_epoch)) = (
                assignment_lock.resource_uid().map(str::to_owned),
                assignment_lock.assignment_epoch().map(str::to_owned),
            ) else {
                assignment_lock.rollback().await?;
                return Err(SessionRuntimeError::SandboxAssignmentChanged);
            };
            if !stop_exact_and_confirm(&self.sandbox_runtime.manager, sandbox_id, &resource_uid)
                .await?
                .0
            {
                assignment_lock.rollback().await?;
                return Err(SessionRuntimeError::SandboxAssignmentChanged);
            }
            let Some(session) = assignment_lock
                .switch_harness_and_commit(harness_type)
                .await?
            else {
                return Err(SessionRuntimeError::SandboxAssignmentChanged);
            };
            remove_pipe_for_assignment(
                &self.sandbox_pipes,
                sandbox_id,
                &resource_uid,
                &assignment_epoch,
            );
            self.store
                .append_event(
                    thread_key,
                    None,
                    "session.harness_switched",
                    json!({
                        "thread_key": thread_key.as_str(),
                        "from_harness": previous_harness,
                        "to_harness": harness_type.as_ref(),
                        "stopped_sandbox_id": previous.sandbox_id,
                    }),
                )
                .await?;
            info!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "session_harness_switched",
                thread_key = %thread_key,
                from_harness = previous_harness,
                to_harness = %harness_type,
                stopped_sandbox_id = sandbox_id,
                "restarted session on a new harness"
            );
            return Ok(session);
        }
        let session = self
            .store
            .switch_session_harness(thread_key, harness_type)
            .await?;
        self.store
            .append_event(
                thread_key,
                None,
                "session.harness_switched",
                json!({
                    "thread_key": thread_key.as_str(),
                    "from_harness": previous_harness,
                    "to_harness": harness_type.as_ref(),
                    "stopped_sandbox_id": previous.sandbox_id,
                }),
            )
            .await?;
        info!(
            component = COMPONENT_SESSION_RUNTIME,
            event = "session_harness_switched",
            thread_key = %thread_key,
            from_harness = previous_harness,
            to_harness = %harness_type,
            stopped_sandbox_id = previous.sandbox_id.as_deref().unwrap_or(""),
            "restarted session on a new harness"
        );
        Ok(session)
    }

    pub async fn append_messages(
        &self,
        thread_key: &ThreadKey,
        messages: &[SessionMessageInput],
    ) -> Result<Vec<String>, SessionRuntimeError> {
        let span = info_span!(
            "centaur.api_rs.session.messages.append",
            component = COMPONENT_SESSION_RUNTIME,
            event = "session_messages_append",
            "centaur.thread_key" = thread_key.as_str(),
            thread_key = %thread_key,
            message_count = messages.len(),
        );
        set_span_parent_trace(
            &span,
            &thread_trace_id(thread_key),
            &thread_trace_parent_span_id(thread_key),
        );
        let result = async {
            ensure_thread_trace_root_span(thread_key);
            if messages.is_empty() {
                return Err(SessionRuntimeError::BadRequest(
                    "messages must not be empty".to_owned(),
                ));
            }
            info!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "session_messages_append_started",
                thread_key = %thread_key,
                message_count = messages.len(),
                "appending session messages"
            );
            let message_ids = self
                .append_messages_with_input_delivery(thread_key, messages)
                .await?;
            if let Err(error) = self.store.touch_session_sandbox_activity(thread_key).await {
                warn!(
                    component = COMPONENT_SESSION_RUNTIME,
                    event = "session_sandbox_activity_touch_failed",
                    thread_key = %thread_key,
                    %error,
                    "failed to touch sandbox activity after message append"
                );
            }
            info!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "session_messages_append_completed",
                thread_key = %thread_key,
                message_count = messages.len(),
                message_id_count = message_ids.len(),
                "session messages appended"
            );
            Ok(message_ids)
        }
        .instrument(span)
        .await;

        let message_ids = match result {
            Ok(message_ids) => message_ids,
            Err(error) => {
                error!(
                    component = COMPONENT_SESSION_RUNTIME,
                    event = "session_messages_append_failed",
                    thread_key = %thread_key,
                    message_count = messages.len(),
                    %error,
                    "failed to append session messages"
                );
                return Err(error);
            }
        };
        self.spawn_session_title_generation(thread_key);
        Ok(message_ids)
    }

    fn spawn_session_title_generation(&self, thread_key: &ThreadKey) {
        let Some(generator) = self.session_title_generator.clone() else {
            return;
        };
        if !self.session_title_in_flight.insert(thread_key.clone()) {
            self.session_title_rerun_requested
                .insert(thread_key.clone());
            return;
        }
        let store = self.store.clone();
        let in_flight = self.session_title_in_flight.clone();
        let rerun_requested = self.session_title_rerun_requested.clone();
        let thread_key = thread_key.clone();
        tokio::spawn(async move {
            // Appends skipped while generation is in flight request one more pass,
            // which lets low-signal wakeups defer to a later substantive message.
            loop {
                rerun_requested.remove(&thread_key);
                maybe_generate_session_title(store.clone(), generator.clone(), thread_key.clone())
                    .await;
                if rerun_requested.remove(&thread_key).is_some() {
                    continue;
                }

                in_flight.remove(&thread_key);
                if rerun_requested.remove(&thread_key).is_some()
                    && in_flight.insert(thread_key.clone())
                {
                    continue;
                }
                break;
            }
        });
    }

    /// Stop every non-terminal sandbox the backend currently owns.
    ///
    /// Intended for a clean control-plane shutdown (e.g. before a deploy):
    /// each sandbox is stopped independently so one failure does not abort the
    /// rest, and the [`DrainReport`] records which were stopped and which
    /// failed so the caller can surface partial failure.
    pub async fn drain(&self) -> Result<DrainReport, SessionRuntimeError> {
        let observed = self.sandbox_runtime.manager.list_observed().await?;
        let mut report = DrainReport::default();
        for sandbox in observed {
            if sandbox.status.is_terminal() {
                continue;
            }
            let id = sandbox.id.as_str().to_owned();
            let Some(resource_uid) = sandbox.resource_uid.as_deref() else {
                report.failed.push(DrainFailure {
                    sandbox_id: id,
                    error: "sandbox drain requires a stable resource UID".to_owned(),
                });
                continue;
            };
            match self
                .sandbox_runtime
                .manager
                .stop_exact(&sandbox.id, Some(resource_uid))
                .await
            {
                Ok(()) => {
                    // A process-wide drain has an observed resource UID but
                    // neither an assignment epoch nor a reconciliation lock.
                    // Do not remove a same-name pipe or name-update a possible
                    // warm replacement; normal reconciliation retires the
                    // stopped resource after the control plane restarts.
                    report.stopped.push(id);
                }
                Err(error) => {
                    warn!(sandbox_id = %id, %error, "drain failed to stop sandbox");
                    report.failed.push(DrainFailure {
                        sandbox_id: id,
                        error: error.to_string(),
                    });
                }
            }
        }
        Ok(report)
    }

    pub async fn stop_workflow_owned_sandboxes(
        &self,
        workflow_run_id: &str,
        reason: &str,
    ) -> Result<WorkflowSandboxCleanupReport, SessionRuntimeError> {
        let sandboxes = self
            .store
            .list_workflow_owned_sandboxes(workflow_run_id)
            .await?;
        let mut report = WorkflowSandboxCleanupReport::default();

        for sandbox in sandboxes {
            let sandbox_id = sandbox.sandbox_id;
            let thread_key = sandbox.thread_key;
            let Some(assignment_lock) = self
                .store
                .lock_sandbox_assignment_for_reconciliation(&thread_key, &sandbox_id)
                .await?
            else {
                continue;
            };
            if assignment_lock.resource_uid() != sandbox.resource_uid.as_deref()
                || assignment_lock.assignment_epoch() != sandbox.assignment_epoch.as_deref()
            {
                assignment_lock.rollback().await?;
                continue;
            }
            let (Some(resource_uid), Some(assignment_epoch)) = (
                sandbox.resource_uid.as_deref(),
                sandbox.assignment_epoch.as_deref(),
            ) else {
                assignment_lock.rollback().await?;
                report.failed.push(DrainFailure {
                    sandbox_id: sandbox_id.clone(),
                    error: "workflow sandbox assignment lacks a stable identity".to_owned(),
                });
                continue;
            };
            let id = SandboxId::new(sandbox_id.clone());
            let missing = match stop_exact_and_confirm(
                &self.sandbox_runtime.manager,
                id.as_str(),
                resource_uid,
            )
            .await
            {
                Ok((true, missing_on_stop)) => {
                    if missing_on_stop {
                        report.missing.push(sandbox_id.clone());
                    } else {
                        report.stopped.push(sandbox_id.clone());
                    }
                    missing_on_stop
                }
                Ok((false, _)) => {
                    assignment_lock.rollback().await?;
                    report.failed.push(DrainFailure {
                        sandbox_id: sandbox_id.clone(),
                        error: "exact workflow sandbox stop was not observable".to_owned(),
                    });
                    continue;
                }
                Err(error) => {
                    assignment_lock.rollback().await?;
                    let error = error.to_string();
                    warn!(
                        thread_key = %thread_key,
                        sandbox_id,
                        workflow_run_id,
                        reason,
                        %error,
                        "failed to stop workflow-owned sandbox"
                    );
                    report.failed.push(DrainFailure {
                        sandbox_id: sandbox_id.clone(),
                        error: error.clone(),
                    });
                    if let Err(event_error) = self
                        .store
                        .append_event(
                            &thread_key,
                            None,
                            "session.workflow_sandbox_stop_failed",
                            json!({
                                "thread_key": thread_key.as_str(),
                                "sandbox_id": sandbox_id,
                                "workflow_run_id": workflow_run_id,
                                "reason": reason,
                                "error": error,
                            }),
                        )
                        .await
                    {
                        warn!(
                            thread_key = %thread_key,
                            sandbox_id,
                            workflow_run_id,
                            %event_error,
                            "failed to append workflow sandbox stop failure event"
                        );
                    }
                    continue;
                }
            };

            if let Err(error) = self
                .store
                .mark_claimed_warm_sandbox_failed_for_assignment(
                    &thread_key,
                    &sandbox_id,
                    resource_uid,
                    assignment_epoch,
                    "workflow-owned sandbox stopped",
                )
                .await
            {
                warn!(
                    thread_key = %thread_key,
                    sandbox_id,
                    workflow_run_id,
                    %error,
                    "failed to mark workflow-owned warm sandbox failed"
                );
            }

            let cleared = assignment_lock.clear_and_commit().await?;
            if cleared {
                remove_pipe_for_assignment(
                    &self.sandbox_pipes,
                    &sandbox_id,
                    resource_uid,
                    assignment_epoch,
                );
            }
            if let Err(error) = self
                .store
                .append_event(
                    &thread_key,
                    None,
                    "session.workflow_sandbox_stopped",
                    json!({
                        "thread_key": thread_key.as_str(),
                        "sandbox_id": sandbox_id,
                        "workflow_run_id": workflow_run_id,
                        "reason": reason,
                        "missing": missing,
                        "cleared": cleared,
                    }),
                )
                .await
            {
                warn!(
                    thread_key = %thread_key,
                    sandbox_id,
                    workflow_run_id,
                    %error,
                    "failed to append workflow sandbox cleanup event"
                );
            }
        }

        Ok(report)
    }

    pub async fn execute_session(
        &self,
        thread_key: &ThreadKey,
        input: ExecuteSessionInput,
    ) -> Result<SessionExecution, SessionRuntimeError> {
        let ExecuteSessionInput {
            idempotency_key,
            metadata,
            input_lines,
            idle_timeout_ms,
            max_duration_ms,
        } = input;
        let input_line_count = input_lines.len();
        let idempotency_key_present = idempotency_key.is_some();
        let span = info_span!(
            "centaur.api_rs.session.execute",
            component = COMPONENT_SESSION_RUNTIME,
            event = "session_execute",
            "centaur.thread_key" = thread_key.as_str(),
            "centaur.execution_id" = tracing::field::Empty,
            "centaur.sandbox_id" = tracing::field::Empty,
            thread_key = %thread_key,
            execution_id = tracing::field::Empty,
            sandbox_id = tracing::field::Empty,
            input_line_count,
            idempotency_key_present,
        );
        set_span_parent_trace(
            &span,
            &thread_trace_id(thread_key),
            &thread_trace_parent_span_id(thread_key),
        );
        let result = async {
            ensure_thread_trace_root_span(thread_key);
            info!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "session_execute_started",
                thread_key = %thread_key,
                input_line_count,
                idempotency_key_present,
                "starting session execution"
            );
            let session = self.store.get_session(thread_key).await?;
            let harness_label = session.harness_type.to_string();
            validate_input_lines(&input_lines)?;
            let (idle_timeout, max_duration) = duration_options(idle_timeout_ms, max_duration_ms)?;

            let desired_capabilities = self
                .resolve_sandbox_capabilities(
                    thread_key,
                    &session.harness_type,
                    session.iron_control_principal.as_deref(),
                    metadata.as_ref(),
                )
                .await?;
            let mut durable_metadata =
                execution_metadata(metadata.clone(), idle_timeout_ms, max_duration_ms);
            merge_json_object(
                &mut durable_metadata,
                metadata_trace_execution_boundary(&desired_capabilities),
            );
            let trace = SessionTraceContext::new(thread_key, None);
            let durable_input_lines =
                input_lines_with_session_context(thread_key, &trace, &input_lines);
            let execution_idempotency_key = idempotency_key
                .clone()
                .unwrap_or_else(|| format!("execute-{}", Uuid::new_v4().simple()));
            let prepared = PreparedInputDelivery {
                idempotency_key: format!("execute:{execution_idempotency_key}"),
                message_ids: Vec::new(),
                input_lines: durable_input_lines,
                boundary_fingerprint: input_delivery_boundary_fingerprint(
                    thread_key,
                    metadata.as_ref(),
                    &desired_capabilities,
                ),
            };
            let created = self
                .store
                .create_execution_with_initial_input_delivery(
                    thread_key,
                    &execution_idempotency_key,
                    durable_metadata,
                    &prepared,
                )
                .await?;
            span.record(
                "centaur.execution_id",
                created.execution.execution_id.as_str(),
            );
            span.record("execution_id", created.execution.execution_id.as_str());
            if !created.created && created.delivery.state == InputDeliveryState::Failed {
                return Err(SessionRuntimeError::BadRequest(format!(
                    "input delivery {} permanently failed: {}",
                    created.delivery.delivery_id,
                    created
                        .delivery
                        .last_error
                        .as_deref()
                        .unwrap_or("unknown failure")
                )));
            }
            if !created.created && created.delivery.state == InputDeliveryState::Flushed {
                info!(
                    component = COMPONENT_SESSION_RUNTIME,
                    event = "session_execute_idempotent_replay",
                    thread_key = %thread_key,
                    execution_id = %created.execution.execution_id,
                    status = %created.execution.status,
                    "returning existing execution"
                );
                return Ok(created.execution);
            }
            let Some(claim) = self
                .claim_input_delivery(
                    Some(&created.execution.execution_id),
                    Some(&created.delivery.delivery_id),
                )
                .await?
            else {
                return Ok(created.execution);
            };
            let execution = claim.execution.clone();
            span.record("centaur.execution_id", execution.execution_id.as_str());
            span.record("execution_id", execution.execution_id.as_str());
            let execution_trace_span = info_span!(
                "centaur.api_rs.session.execution",
                component = COMPONENT_SESSION_RUNTIME,
                event = "session_execution",
                "centaur.thread_key" = thread_key.as_str(),
                "centaur.execution_id" = execution.execution_id.as_str(),
                "centaur.sandbox_id" = tracing::field::Empty,
                thread_key = %thread_key,
                execution_id = %execution.execution_id,
                sandbox_id = tracing::field::Empty,
            );
            set_span_parent_trace(
                &execution_trace_span,
                &thread_trace_id(thread_key),
                &thread_trace_parent_span_id(thread_key),
            );
            self.execution_spans
                .lock()
                .await
                .insert(execution.execution_id.clone(), execution_trace_span.clone());
            record_session_execution_started(&harness_label);
            if claim.delivery.attempts == 1 {
                self.store
                    .append_event(
                        thread_key,
                        Some(&execution.execution_id),
                        "session.execution_started",
                        json!({
                            "execution_id": execution.execution_id,
                            "thread_key": thread_key.as_str(),
                            "input_line_count": input_line_count,
                            "idle_timeout_ms": idle_timeout_ms,
                            "max_duration_ms": max_duration_ms,
                        }),
                    )
                    .await?;
            }
            if let Err(error) = self.drive_claimed_input_delivery(&claim).await {
                self.finish_failed_input_delivery(&claim, &error).await;
                return Err(error);
            }

            let delivered_session = self.store.get_session(thread_key).await?;
            if let Some(sandbox_id) = delivered_session.sandbox_id.as_deref() {
                span.record("centaur.sandbox_id", sandbox_id);
                span.record("sandbox_id", sandbox_id);
                execution_trace_span.record("centaur.sandbox_id", sandbox_id);
                execution_trace_span.record("sandbox_id", sandbox_id);
            }

            if let Some(max_duration) = max_duration {
                spawn_max_duration_failure(
                    self.context(),
                    thread_key.clone(),
                    execution.execution_id.clone(),
                    max_duration,
                    idle_timeout,
                );
            }

            info!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "session_execute_completed",
                thread_key = %thread_key,
                execution_id = %execution.execution_id,
                sandbox_id = delivered_session.sandbox_id.as_deref().unwrap_or(""),
                status = %execution.status,
                completion_reason = "input_accepted",
                "session execution accepted input"
            );
            Ok(execution)
        }
        .instrument(span.clone())
        .await;

        if let Err(error) = &result {
            error!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "session_execute_failed",
                thread_key = %thread_key,
                input_line_count,
                %error,
                "session execution failed"
            );
        }
        result
    }

    #[cfg(test)]
    async fn record_execution_failure(
        &self,
        thread_key: &ThreadKey,
        execution_id: &str,
        error: &SessionRuntimeError,
    ) {
        let error_message = error.to_string();
        let payload = json!({
            "execution_id": execution_id,
            "thread_key": thread_key.as_str(),
            "error": error_message,
        });
        let execution = match self
            .store
            .terminalize_execution_and_append_event_if_stdout_owner(
                execution_id,
                &self.stdout_owner_id,
                OwnedTerminalEvent::Failed {
                    error: error_message,
                    payload,
                },
            )
            .await
        {
            Ok(Some((execution, _))) => execution,
            Ok(None) => return,
            Err(_) => return,
        };
        stop_terminal_stdout_owner_renewer(&self.context(), execution_id).await;
        self.execution_spans.lock().await.remove(execution_id);
        record_finished_execution_metric(
            &self.store,
            thread_key,
            &execution,
            "failed",
            Some(runtime_error_failure_class(error)),
        )
        .await;
    }

    async fn append_messages_with_input_delivery(
        &self,
        thread_key: &ThreadKey,
        messages: &[SessionMessageInput],
    ) -> Result<Vec<String>, SessionRuntimeError> {
        if messages
            .iter()
            .filter(|message| matches!(message.role, MessageRole::User))
            .map(|message| {
                SlackTraceSubject::from_execution_metadata(
                    thread_key.as_str(),
                    Some(&message.metadata),
                )
                .map(|subject| subject.stable_key())
            })
            .collect::<BTreeSet<_>>()
            .len()
            > 1
        {
            let mut message_ids = Vec::with_capacity(messages.len());
            for message in messages {
                message_ids.extend(
                    Box::pin(self.append_messages_with_input_delivery(
                        thread_key,
                        std::slice::from_ref(message),
                    ))
                    .await?,
                );
            }
            return Ok(message_ids);
        }
        let prepared_messages = prepare_session_messages(thread_key, messages);
        let message_ids = prepared_messages
            .iter()
            .map(|message| message.message_id.clone())
            .collect::<Vec<_>>();
        let active = match self
            .store
            .append_prepared_messages_if_no_active_execution(thread_key, &prepared_messages)
            .await?
        {
            AppendMessagesWithoutActiveExecution::Appended(message_ids) => return Ok(message_ids),
            AppendMessagesWithoutActiveExecution::Active(active) => active,
        };

        let session = self.store.get_session(thread_key).await?;
        let actor_metadata = messages
            .iter()
            .find(|message| matches!(message.role, MessageRole::User))
            .map(|message| &message.metadata);
        let capabilities = self
            .resolve_sandbox_capabilities(
                thread_key,
                &session.harness_type,
                session.iron_control_principal.as_deref(),
                actor_metadata,
            )
            .await?;
        let trace = SessionTraceContext::new(thread_key, None);
        let input_lines = input_lines_with_session_context(
            thread_key,
            &trace,
            &steering_input_lines(thread_key, messages, &message_ids),
        );
        if input_lines.is_empty() {
            return self
                .store
                .append_messages(thread_key, messages)
                .await
                .map_err(Into::into);
        }
        let prepared = PreparedInputDelivery {
            idempotency_key: input_delivery_idempotency_key(thread_key, &prepared_messages),
            message_ids: message_ids.clone(),
            input_lines,
            boundary_fingerprint: input_delivery_boundary_fingerprint(
                thread_key,
                actor_metadata,
                &capabilities,
            ),
        };

        let delivery = if messages_match_active_trace_subject(thread_key, Some(&active), messages)
            && execution_trace_boundary_matches_capabilities(&active, &capabilities)
        {
            match self
                .store
                .append_messages_and_enqueue_input_delivery(
                    thread_key,
                    &active.execution_id,
                    &prepared_messages,
                    &prepared,
                )
                .await?
            {
                Some(delivery) => delivery,
                None => return Err(SessionRuntimeError::MetadataTraceBoundaryChanged),
            }
        } else {
            return self
                .replace_trace_boundary_execution(
                    thread_key,
                    &active,
                    &prepared_messages,
                    actor_metadata.cloned(),
                    prepared,
                )
                .await
                .map(|()| message_ids);
        };

        if delivery.state == InputDeliveryState::Failed {
            return Err(SessionRuntimeError::BadRequest(format!(
                "input delivery {} permanently failed: {}",
                delivery.delivery_id,
                delivery.last_error.as_deref().unwrap_or("unknown failure")
            )));
        }
        if delivery.state != InputDeliveryState::Flushed
            && let Some(claim) = self
                .claim_input_delivery(Some(&active.execution_id), Some(&delivery.delivery_id))
                .await?
            && let Err(error) = self.drive_claimed_input_delivery(&claim).await
        {
            self.finish_failed_input_delivery(&claim, &error).await;
            return Err(error);
        }
        Ok(delivery.message_ids)
    }

    /// A different Slack actor's durable message must not disappear merely
    /// because it arrived while a consented actor owned the thread. Retire
    /// that exact sandbox, terminalize its old execution, then start a fresh
    /// execution from the already-persisted message under the new actor's
    /// capabilities (normally untraced).
    async fn replace_trace_boundary_execution(
        &self,
        thread_key: &ThreadKey,
        active: &SessionExecution,
        messages: &[PreparedSessionMessage],
        metadata: Option<Value>,
        prepared: PreparedInputDelivery,
    ) -> Result<(), SessionRuntimeError> {
        let boundary = SessionRuntimeError::MetadataTraceBoundaryChanged;
        let session = self.store.get_session(thread_key).await?;
        let capabilities = self
            .resolve_sandbox_capabilities(
                thread_key,
                &session.harness_type,
                session.iron_control_principal.as_deref(),
                metadata.as_ref(),
            )
            .await?;
        let mut successor_metadata = execution_metadata(metadata.clone(), None, None);
        merge_json_object(
            &mut successor_metadata,
            metadata_trace_execution_boundary(&capabilities),
        );
        let terminalized = self
            .store
            .replace_active_execution_with_initial_input_delivery(
                &active.execution_id,
                messages,
                successor_metadata,
                OwnedTerminalEvent::Failed {
                    error: boundary.to_string(),
                    payload: json!({
                        "execution_id": active.execution_id,
                        "thread_key": thread_key.as_str(),
                        "error": boundary.to_string(),
                    }),
                },
                &prepared,
            )
            .await?;
        let Some((terminalized, successor, delivery)) = terminalized else {
            // Another replica may have won the terminal CAS.  Do not create a
            // second active row unless this caller observed its own durable
            // replacement boundary.
            return Err(SessionRuntimeError::MetadataTraceBoundaryChanged);
        };
        if let Some(sandbox_id) = session.sandbox_id.as_deref() {
            self.discard_sandbox_before_input(thread_key, sandbox_id)
                .await?;
        }
        stop_terminal_stdout_owner_renewer(&self.context(), &active.execution_id).await;
        self.execution_spans
            .lock()
            .await
            .remove(&active.execution_id);
        record_finished_execution_metric(
            &self.store,
            thread_key,
            &terminalized,
            "failed",
            Some(runtime_error_failure_class(&boundary)),
        )
        .await;
        let Some(claim) = self
            .claim_input_delivery(Some(&successor.execution_id), Some(&delivery.delivery_id))
            .await?
        else {
            return Ok(());
        };
        if let Err(error) = self.drive_claimed_input_delivery(&claim).await {
            self.finish_failed_input_delivery(&claim, &error).await;
            return Err(error);
        }
        let _ = self
            .store
            .append_event(
                thread_key,
                Some(&successor.execution_id),
                "session.steering_replaced_trace_boundary",
                json!({
                    "replaced_execution_id": active.execution_id,
                    "replacement_execution_id": successor.execution_id,
                    "message_ids": prepared.message_ids,
                    "trace_enabled": capabilities.metadata_trace_enabled,
                }),
            )
            .await;
        Ok(())
    }

    pub async fn interrupt_active_execution(
        &self,
        thread_key: &ThreadKey,
        reason: &str,
        execution_metadata: Option<&Value>,
    ) -> Result<InterruptExecutionOutcome, SessionRuntimeError> {
        let Some(execution) = self.store.active_execution_for_thread(thread_key).await? else {
            return Ok(InterruptExecutionOutcome {
                interrupted: false,
                execution_id: None,
            });
        };

        let session = self.store.get_session(thread_key).await?;
        if let Some(capabilities) = session
            .sandbox_capabilities
            .as_ref()
            .filter(|capabilities| capabilities.metadata_trace_enabled)
        {
            let sandbox_id = session
                .sandbox_id
                .as_deref()
                .ok_or(SessionRuntimeError::MetadataTraceBoundaryChanged)?;
            let requested =
                SlackTraceSubject::from_execution_metadata(thread_key.as_str(), execution_metadata);
            let assigned = self
                .store
                .metadata_trace_assignment_actor(thread_key, sandbox_id)
                .await?;
            let matches_assignment =
                requested
                    .zip(assigned)
                    .is_some_and(|(requested, assigned)| {
                        assigned.source == "slack"
                            && assigned.workspace_id == requested.workspace_id()
                            && assigned.user_id == requested.user_id()
                    });
            if !matches_assignment {
                self.discard_sandbox_before_input(thread_key, sandbox_id)
                    .await?;
                return Err(SessionRuntimeError::MetadataTraceBoundaryChanged);
            }
            // Keep this binding in scope through the later trace write so the
            // compiler documents that a trace interrupt was actor-checked.
            let _ = capabilities;
        }

        let capabilities = self
            .resolve_sandbox_capabilities(
                thread_key,
                &session.harness_type,
                session.iron_control_principal.as_deref(),
                execution_metadata,
            )
            .await?;
        let trace = SessionTraceContext::new(thread_key, None);
        let input_lines = input_lines_with_session_context(
            thread_key,
            &trace,
            &[interrupt_input_line(thread_key, reason)],
        );
        let prepared = PreparedInputDelivery {
            idempotency_key: format!(
                "interrupt:{}:{}",
                execution.execution_id,
                Uuid::new_v4().simple()
            ),
            message_ids: Vec::new(),
            input_lines,
            boundary_fingerprint: input_delivery_boundary_fingerprint(
                thread_key,
                execution_metadata,
                &capabilities,
            ),
        };
        let delivery = self
            .store
            .append_messages_and_enqueue_input_delivery(
                thread_key,
                &execution.execution_id,
                &[],
                &prepared,
            )
            .await?
            .ok_or(SessionRuntimeError::MetadataTraceBoundaryChanged)?;
        let Some(claim) = self
            .claim_input_delivery(Some(&execution.execution_id), Some(&delivery.delivery_id))
            .await?
        else {
            return Err(SessionRuntimeError::MetadataTraceBoundaryChanged);
        };
        if let Err(error) = self.drive_claimed_input_delivery(&claim).await {
            self.finish_failed_input_delivery(&claim, &error).await;
            return Err(error);
        }

        self.store
            .append_event(
                thread_key,
                Some(&execution.execution_id),
                "session.interrupt_delivered",
                json!({
                    "execution_id": execution.execution_id,
                    "thread_key": thread_key.as_str(),
                    "reason": reason,
                }),
            )
            .await?;

        Ok(InterruptExecutionOutcome {
            interrupted: true,
            execution_id: Some(execution.execution_id),
        })
    }

    pub async fn stream_events(
        &self,
        thread_key: &ThreadKey,
        after_event_id: i64,
        execution_id: Option<&str>,
    ) -> Result<
        impl Stream<Item = Result<SessionEvent, SessionRuntimeError>> + use<>,
        SessionRuntimeError,
    > {
        let span = info_span!(
            "centaur.api_rs.session.events.stream",
            component = COMPONENT_SESSION_RUNTIME,
            event = "session_events_stream",
            "centaur.thread_key" = thread_key.as_str(),
            thread_key = %thread_key,
            after_event_id,
            execution_id = execution_id.unwrap_or(""),
        );
        let result = async {
            info!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "session_events_stream_started",
                thread_key = %thread_key,
                after_event_id,
                execution_id = execution_id.unwrap_or(""),
                "opening session event stream"
            );
            let session = self.store.get_session(thread_key).await?;
            if let Some(sandbox_id) = session.sandbox_id.as_deref() {
                self.ensure_session_pipe_if_live(thread_key, sandbox_id)
                    .await?;
            }

            let listener = self.store.listen_session_events().await?;

            Ok(session_event_stream(
                self.store.clone(),
                thread_key.clone(),
                after_event_id,
                execution_id.map(ToOwned::to_owned),
                listener,
                span.clone(),
            ))
        }
        .instrument(span.clone())
        .await;

        if let Err(error) = &result {
            error!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "session_events_stream_failed",
                thread_key = %thread_key,
                after_event_id,
                %error,
                "failed to open session event stream"
            );
        }
        result
    }

    async fn ensure_session_sandbox(
        &self,
        request: EnsureSessionSandboxRequest<'_>,
    ) -> Result<String, SessionRuntimeError> {
        let EnsureSessionSandboxRequest {
            thread_key,
            harness_type,
            persona_id,
            existing_sandbox_id,
            existing_sandbox_capabilities,
            iron_control_principal,
            proxy_labels,
            desired_capabilities,
            execution_metadata,
            execution_id,
        } = request;
        let boot_mode = sandbox_boot_mode_for_thread(thread_key, iron_control_principal);
        let span = info_span!(
            "centaur.api_rs.sandbox.ensure",
            component = COMPONENT_SESSION_RUNTIME,
            event = "sandbox_ensure",
            "centaur.thread_key" = thread_key.as_str(),
            "centaur.execution_id" = execution_id,
            "centaur.sandbox_id" = tracing::field::Empty,
            thread_key = %thread_key,
            execution_id,
            sandbox_id = tracing::field::Empty,
            existing_sandbox_id = existing_sandbox_id.unwrap_or(""),
            iron_control_principal_present = iron_control_principal.is_some(),
            persona_id = persona_id.unwrap_or(""),
            sandbox_boot_mode = boot_mode.as_str(),
            sandbox_repo_cache_access = desired_capabilities.repo_cache.as_str(),
            sandbox_repo_cache_enabled = desired_capabilities.repo_cache_enabled(),
            sandbox_observability_enabled = desired_capabilities.observability_enabled,
            sandbox_api_server_enabled = desired_capabilities.api_server_enabled,
        );
        let ensure_started = Instant::now();
        let result = async {
            self.ensure_metadata_trace_config_active(desired_capabilities)
                .await?;
            let persona_context =
                self.resolve_stored_persona(persona_id, harness_type, desired_capabilities)?;
            if let Some(sandbox_id) = existing_sandbox_id {
                let id = SandboxId::new(sandbox_id);
                if !sandbox_capabilities_match(existing_sandbox_capabilities, desired_capabilities)
                {
                    self.discard_sandbox_before_input(thread_key, sandbox_id)
                        .await?;
                    self.store
                        .append_event(
                            thread_key,
                            Some(execution_id),
                            "session.sandbox_capabilities_replaced",
                            json!({
                                "execution_id": execution_id,
                                "thread_key": thread_key.as_str(),
                                "sandbox_id": sandbox_id,
                                "previous_capabilities": existing_sandbox_capabilities,
                                "desired_capabilities": desired_capabilities,
                            }),
                        )
                        .await?;
                    info!(
                        component = COMPONENT_SESSION_RUNTIME,
                        event = "sandbox_ensure_capabilities_replaced",
                        thread_key = %thread_key,
                        execution_id,
                        sandbox_id,
                        sandbox_repo_cache_access = desired_capabilities.repo_cache.as_str(),
                        sandbox_repo_cache_enabled = desired_capabilities.repo_cache_enabled(),
                        sandbox_observability_enabled = desired_capabilities.observability_enabled,
                        sandbox_api_server_enabled = desired_capabilities.api_server_enabled,
                        "replacing existing sandbox whose capabilities do not match"
                    );
                } else {
                    match self.sandbox_runtime.manager.status(&id).await {
                        Ok(status) => match existing_sandbox_action(&status) {
                            ExistingSandboxAction::Reuse => {
                                let fenced = self
                                    .run_reuse_fence_with_capacity(
                                        &id,
                                        thread_key,
                                        execution_id,
                                        || async {
                                            fence_running_assignment(
                                                &self.store,
                                                &self.sandbox_runtime.manager,
                                                thread_key,
                                                sandbox_id,
                                            )
                                            .await
                                        },
                                    )
                                    .await?;
                                if !fenced {
                                    return Err(SessionRuntimeError::SandboxAssignmentChanged);
                                }
                                if let Some(principal_id) = iron_control_principal {
                                    self.sandbox_runtime
                                        .manager
                                        .ensure_iron_control_proxy_resources(
                                            &id,
                                            principal_id,
                                            proxy_labels,
                                        )
                                        .await?;
                                }
                                span.record("centaur.sandbox_id", sandbox_id);
                                span.record("sandbox_id", sandbox_id);
                                let ready_duration = ensure_started.elapsed();
                                self.record_sandbox_ready(SandboxReadyObservation {
                                    thread_key,
                                    execution_id,
                                    sandbox_id,
                                    harness_type,
                                    source: "reused",
                                    ready_duration,
                                    startup_duration: None,
                                })
                                .await;
                                info!(
                                    component = COMPONENT_SESSION_RUNTIME,
                                    event = "sandbox_ensure_reused",
                                    thread_key = %thread_key,
                                    execution_id,
                                    sandbox_id,
                                    harness_type = %harness_type,
                                    sandbox_ready_source = "reused",
                                    sandbox_ready_duration_ms = duration_millis_u64(ready_duration),
                                    "reusing existing session sandbox"
                                );
                                return Ok(sandbox_id.to_owned());
                            }
                            ExistingSandboxAction::ResumeOrReplace => {
                                let fenced = self
                                    .run_reuse_fence_with_capacity(
                                        &id,
                                        thread_key,
                                        execution_id,
                                        || async {
                                            fence_running_assignment(
                                                &self.store,
                                                &self.sandbox_runtime.manager,
                                                thread_key,
                                                sandbox_id,
                                            )
                                            .await
                                        },
                                    )
                                    .await?;
                                if !fenced {
                                    return Err(SessionRuntimeError::SandboxAssignmentChanged);
                                }
                                if let Some(principal_id) = iron_control_principal {
                                    self.sandbox_runtime
                                        .manager
                                        .ensure_iron_control_proxy_resources(
                                            &id,
                                            principal_id,
                                            proxy_labels,
                                        )
                                        .await?;
                                }
                                span.record("centaur.sandbox_id", sandbox_id);
                                span.record("sandbox_id", sandbox_id);
                                let ready_duration = ensure_started.elapsed();
                                self.record_sandbox_ready(SandboxReadyObservation {
                                    thread_key,
                                    execution_id,
                                    sandbox_id,
                                    harness_type,
                                    source: "resumed",
                                    ready_duration,
                                    startup_duration: None,
                                })
                                .await;
                                return Ok(sandbox_id.to_owned());
                            }
                            ExistingSandboxAction::Replace => {
                                info!(
                                    component = COMPONENT_SESSION_RUNTIME,
                                    event = "sandbox_ensure_replacing",
                                    thread_key = %thread_key,
                                    execution_id,
                                    sandbox_id,
                                    status = ?status,
                                    "existing sandbox is not reusable"
                                );
                                // A Kubernetes sandbox with an accepted delete
                                // is reported as Gone while its cleanup finalizer
                                // still retains the exact auxiliary generation.
                                // Finish that retirement and clear the durable
                                // assignment before a replacement can overwrite
                                // the only retry handle.
                                self.discard_sandbox_before_input(thread_key, sandbox_id)
                                    .await?;
                            }
                        },
                        Err(SandboxError::NotFound(_)) => {
                            info!(
                                component = COMPONENT_SESSION_RUNTIME,
                                event = "sandbox_ensure_missing",
                                thread_key = %thread_key,
                                execution_id,
                                sandbox_id,
                                "existing sandbox is missing"
                            );
                        }
                        Err(error) => return Err(SessionRuntimeError::Sandbox(error)),
                    }
                }
            }

            // Warm sandboxes are pre-booted with the workload's default
            // harness; a session on any other harness needs a cold sandbox.
            let warm_harness_matches = self
                .sandbox_runtime
                .warm_harness
                .as_ref()
                .is_none_or(|warm| warm == harness_type);
            let warm_persona_matches = persona_context.is_none();
            if !warm_harness_matches && self.warm_pool.is_some() {
                record_sandbox_warm_pool_claim("harness_mismatch");
            }
            if !warm_persona_matches && self.warm_pool.is_some() {
                record_sandbox_warm_pool_claim("persona_specific");
            }
            if !desired_capabilities.is_default_enabled() && self.warm_pool.is_some() {
                record_sandbox_warm_pool_claim("capabilities_non_default");
            }
            if let Some(warm_pool) = self.warm_pool.as_ref().filter(|_| {
                boot_mode.uses_warm_pool()
                    && warm_harness_matches
                    && warm_persona_matches
                    && desired_capabilities.is_default_enabled()
            }) {
                let expected_assignment =
                    self.store.sandbox_assignment_snapshot(thread_key).await?;
                match warm_pool
                    .claim(thread_key.as_str(), iron_control_principal, proxy_labels)
                    .await
                {
                    Ok(Some(handle)) => {
                        let sandbox_id = handle.id.as_str();
                        let resource_uid = handle
                            .resource_uid
                            .as_deref()
                            .ok_or(SessionRuntimeError::SandboxAssignmentChanged)?;
                        record_sandbox_warm_pool_claim("hit");
                        span.record("centaur.sandbox_id", sandbox_id);
                        span.record("sandbox_id", sandbox_id);
                        let ready_duration = ensure_started.elapsed();
                        if !self
                            .store
                            .update_sandbox_assignment_if_matches(
                                thread_key,
                                sandbox_id,
                                Some(resource_uid),
                                desired_capabilities,
                                &expected_assignment,
                            )
                            .await?
                        {
                            self.stop_created_sandbox_exact(&handle).await;
                            return Err(SessionRuntimeError::SandboxAssignmentChanged);
                        }
                        self.store
                            .append_event(
                                thread_key,
                                None,
                                "session.warm_sandbox_claimed",
                                json!({
                                    "sandbox_id": sandbox_id,
                                    "workload_key": warm_pool.workload_key(),
                                    "iron_control_principal": iron_control_principal,
                                    "sandbox_capabilities": desired_capabilities,
                                }),
                            )
                            .await?;
                        self.record_sandbox_ready(SandboxReadyObservation {
                            thread_key,
                            execution_id,
                            sandbox_id,
                            harness_type,
                            source: "warm_pool",
                            ready_duration,
                            startup_duration: None,
                        })
                        .await;
                        info!(
                            component = COMPONENT_SESSION_RUNTIME,
                            event = "sandbox_ensure_warm_claimed",
                            thread_key = %thread_key,
                            execution_id,
                            sandbox_id = %sandbox_id,
                            harness_type = %harness_type,
                            sandbox_ready_source = "warm_pool",
                            sandbox_ready_duration_ms = duration_millis_u64(ready_duration),
                            workload_key = warm_pool.workload_key(),
                            "claimed warm session sandbox"
                        );
                        return Ok(handle.id.into_string());
                    }
                    Ok(None) => record_sandbox_warm_pool_claim("miss"),
                    Err(error) => {
                        record_sandbox_warm_pool_claim("error");
                        return Err(SessionRuntimeError::WarmPool(error));
                    }
                }
            }

            // Fence the eventual assignment against a concurrent replacement;
            // creating a sidecar is not permission to overwrite a newer
            // actor's sandbox row.
            let expected_assignment = self.store.sandbox_assignment_snapshot(thread_key).await?;
            let mut spec = (self.sandbox_runtime.spec_factory)(
                thread_key,
                execution_id,
                harness_type,
                persona_context.as_ref(),
            );
            if let Some(principal) = iron_control_principal {
                spec.iron_control_principal = Some(principal.to_owned());
                spec.iron_control_proxy_labels = proxy_labels.clone();
            }
            apply_sandbox_boot_mode(&mut spec, &boot_mode);
            apply_sandbox_capabilities(&mut spec, desired_capabilities);
            self.ensure_metadata_trace_config_active(desired_capabilities)
                .await?;
            let create_started = Instant::now();
            let handle = self
                .run_with_running_capacity(thread_key, execution_id, "cold_create", || async {
                    self.sandbox_runtime
                        .manager
                        .create_running(spec)
                        .await
                        .map_err(SessionRuntimeError::Sandbox)
                })
                .await?;
            let startup_duration = create_started.elapsed();
            let ready_duration = ensure_started.elapsed();
            let resource_uid = handle
                .resource_uid
                .as_deref()
                .ok_or(SessionRuntimeError::SandboxAssignmentChanged)?;
            span.record("centaur.sandbox_id", handle.id.as_str());
            span.record("sandbox_id", handle.id.as_str());
            if let Err(error) = self
                .persist_sandbox_assignment(
                    thread_key,
                    handle.id.as_str(),
                    resource_uid,
                    desired_capabilities,
                    &expected_assignment,
                    execution_metadata,
                )
                .await
            {
                self.stop_created_sandbox_exact(&handle).await;
                return Err(error);
            }
            self.record_sandbox_ready(SandboxReadyObservation {
                thread_key,
                execution_id,
                sandbox_id: handle.id.as_str(),
                harness_type,
                source: "cold_create",
                ready_duration,
                startup_duration: Some(startup_duration),
            })
            .await;
            info!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "sandbox_ensure_created",
                thread_key = %thread_key,
                execution_id,
                sandbox_id = %handle.id.as_str(),
                harness_type = %harness_type,
                sandbox_ready_source = "cold_create",
                sandbox_ready_duration_ms = duration_millis_u64(ready_duration),
                sandbox_startup_duration_ms = duration_millis_u64(startup_duration),
                sandbox_startup_duration_seconds = startup_duration.as_secs_f64(),
                "created new session sandbox"
            );
            Ok(handle.id.into_string())
        }
        .instrument(span.clone())
        .await;

        if let Err(error) = &result {
            error!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "sandbox_ensure_failed",
                thread_key = %thread_key,
                execution_id,
                %error,
                "failed to ensure session sandbox"
            );
        }
        result
    }

    async fn resolve_sandbox_capabilities(
        &self,
        thread_key: &ThreadKey,
        harness_type: &HarnessType,
        iron_control_principal: Option<&str>,
        execution_metadata: Option<&Value>,
    ) -> Result<SessionSandboxCapabilities, SessionRuntimeError> {
        let capabilities = match (iron_control_principal, &self.iron_control) {
            (Some(principal_id), Some(registrar)) => {
                sandbox_capabilities_from_principal(&registrar.get_principal(principal_id).await?)
            }
            (Some(_), None) => {
                return Err(SessionRuntimeError::BadRequest(
                    "session has an Iron Control principal, but Iron Control is disabled"
                        .to_owned(),
                ));
            }
            _ => SessionSandboxCapabilities::default_enabled(),
        };
        let Some(subject) =
            SlackTraceSubject::from_execution_metadata(thread_key.as_str(), execution_metadata)
        else {
            return Ok(capabilities);
        };
        if !matches!(harness_type, HarnessType::Codex) {
            // The reviewed metadata trace sidecar is a Codex-only boundary.
            // Do not persist a trace capability that a non-Codex backend could
            // not prove it is exporting through that sidecar.
            return Ok(capabilities);
        }
        let consent = self
            .store
            .metadata_trace_consent("slack", subject.workspace_id(), subject.user_id())
            .await?;
        let mut desired = sandbox_capabilities_with_trace_subject(
            capabilities,
            &subject,
            &consent,
            self.metadata_trace_config.as_ref(),
        );
        if desired.metadata_trace_enabled && !self.metadata_trace_config_is_active().await {
            disable_metadata_trace(&mut desired);
        }
        Ok(desired)
    }

    /// Reconciliation never reuses an execution's actor metadata. It derives
    /// the consent subject from the exact FK captured with this sandbox
    /// assignment; old/null-FK assignments fail closed and are retired.
    async fn metadata_trace_assignment_metadata(
        &self,
        session: &Session,
    ) -> Result<Option<Value>, SessionRuntimeError> {
        let Some(sandbox_id) = session.sandbox_id.as_deref() else {
            return Ok(None);
        };
        let Some(actor) = self
            .store
            .metadata_trace_assignment_actor(&session.thread_key, sandbox_id)
            .await?
        else {
            return Ok(None);
        };
        if actor.source != "slack" {
            return Ok(None);
        }
        Ok(Some(json!({
            "slack_actor_team_id": actor.workspace_id,
            "slack_actor_user_id": actor.user_id,
        })))
    }

    /// A traced assignment is self-authenticating from its durable actor FK,
    /// consent revision/expiry, and deployment generation. Reconciliation
    /// must evaluate this before consulting optional Iron Control so a missing
    /// registrar cannot keep an expired trace sidecar alive.
    async fn persisted_trace_boundary_is_current(
        &self,
        session: &Session,
    ) -> Result<bool, SessionRuntimeError> {
        let Some(capabilities) = session.sandbox_capabilities.as_ref() else {
            return Ok(true);
        };
        if !capabilities.metadata_trace_enabled {
            return Ok(true);
        }
        let Some(identity) = self.metadata_trace_config.as_ref() else {
            return Ok(false);
        };
        if !identity.enabled
            || capabilities.metadata_trace_config_generation != Some(identity.generation)
            || capabilities.metadata_trace_config_fingerprint.as_deref()
                != Some(identity.fingerprint.as_str())
            || !self.store.metadata_trace_config_is_active(identity).await?
        {
            return Ok(false);
        }
        let Some(actor_metadata) = self.metadata_trace_assignment_metadata(session).await? else {
            return Ok(false);
        };
        let Some(subject) = SlackTraceSubject::from_execution_metadata(
            session.thread_key.as_str(),
            Some(&actor_metadata),
        ) else {
            return Ok(false);
        };
        let consent = self
            .store
            .metadata_trace_consent("slack", subject.workspace_id(), subject.user_id())
            .await?;
        let subject_hash = trace_subject_hash(&subject);
        Ok(consent.enabled
            && !consent.drain_pending
            && consent.expires_at == capabilities.metadata_trace_expires_at
            && consent.revision
                == capabilities
                    .metadata_trace_consent_revision
                    .unwrap_or_default()
            && capabilities.metadata_trace_subject_hash.as_deref() == Some(subject_hash.as_str()))
    }

    async fn ensure_metadata_trace_config_active(
        &self,
        capabilities: &SessionSandboxCapabilities,
    ) -> Result<(), SessionRuntimeError> {
        if !capabilities.metadata_trace_enabled {
            return Ok(());
        }
        let Some(identity) = self.metadata_trace_config.as_ref() else {
            return Err(SessionRuntimeError::InactiveMetadataTraceConfig);
        };
        if capabilities.metadata_trace_config_generation != Some(identity.generation)
            || capabilities.metadata_trace_config_fingerprint.as_deref()
                != Some(identity.fingerprint.as_str())
            || !self.store.metadata_trace_config_is_active(identity).await?
        {
            return Err(SessionRuntimeError::InactiveMetadataTraceConfig);
        }
        Ok(())
    }

    #[cfg(test)]
    async fn write_traced_input_lines(
        &self,
        pipe: &SessionPipe,
        thread_key: &ThreadKey,
        execution_id: &str,
        sandbox_id: &str,
        expected: &SessionSandboxCapabilities,
        input_lines: &[String],
    ) -> Result<(), SessionRuntimeError> {
        // Lock the pipe first, then hold the DB shared lock through send+flush.
        // Revoke takes FOR UPDATE on this consent row and therefore linearizes
        // strictly before or after a traced stdin delivery.
        let mut stdin = pipe.stdin.lock().await;
        let (Some(assignment_epoch), Some(resource_uid)) = (
            pipe.trace_assignment_epoch.as_deref(),
            pipe.trace_resource_uid.as_deref(),
        ) else {
            return Err(SessionRuntimeError::MetadataTraceBoundaryChanged);
        };
        let Some(guard) = self
            .store
            .lock_metadata_trace_input(
                expected,
                thread_key,
                execution_id,
                sandbox_id,
                assignment_epoch,
                resource_uid,
            )
            .await?
        else {
            return Err(SessionRuntimeError::MetadataTraceBoundaryChanged);
        };
        let Some(remaining) = guard.remaining() else {
            return Err(SessionRuntimeError::MetadataTraceBoundaryChanged);
        };
        // A valid consent may last hours, but it must never pin its shared
        // DB boundary lock behind an unresponsive stdin pipe for that long.
        let write_timeout = metadata_trace_write_timeout(remaining);
        let write = async {
            for line in input_lines {
                stdin.send(line).await.map_err(codec_error_to_runtime)?;
            }
            io::AsyncWriteExt::flush(stdin.get_mut())
                .await
                .map_err(|error| {
                    SessionRuntimeError::Sandbox(SandboxError::io_source("flush stdin", error))
                })
        };
        timeout(write_timeout, write)
            .await
            .map_err(|_| SessionRuntimeError::MetadataTraceBoundaryChanged)??;
        guard.commit().await?;
        Ok(())
    }

    async fn discard_sandbox_before_input(
        &self,
        thread_key: &ThreadKey,
        sandbox_id: &str,
    ) -> Result<(), SessionRuntimeError> {
        let Some(assignment_lock) = self
            .store
            .lock_sandbox_assignment_for_reconciliation(thread_key, sandbox_id)
            .await?
        else {
            return Ok(());
        };
        let (Some(resource_uid), Some(assignment_epoch)) = (
            assignment_lock.resource_uid().map(str::to_owned),
            assignment_lock.assignment_epoch().map(str::to_owned),
        ) else {
            assignment_lock.rollback().await?;
            return Err(SessionRuntimeError::SandboxAssignmentChanged);
        };
        if !stop_exact_and_confirm(&self.sandbox_runtime.manager, sandbox_id, &resource_uid)
            .await?
            .0
        {
            assignment_lock.rollback().await?;
            return Err(SessionRuntimeError::MetadataTraceBoundaryChanged);
        }
        if assignment_lock.clear_and_commit().await? {
            remove_pipe_for_assignment(
                &self.sandbox_pipes,
                sandbox_id,
                &resource_uid,
                &assignment_epoch,
            );
        }
        Ok(())
    }

    async fn metadata_trace_config_is_active(&self) -> bool {
        let Some(identity) = self.metadata_trace_config.as_ref() else {
            return false;
        };
        self.store
            .metadata_trace_config_is_active(identity)
            .await
            .unwrap_or(false)
    }

    async fn stop_created_sandbox_exact(&self, handle: &SandboxHandle) {
        let Some(resource_uid) = handle.resource_uid.as_deref() else {
            return;
        };
        let _ = self
            .sandbox_runtime
            .manager
            .stop_exact(&handle.id, Some(resource_uid))
            .await;
    }

    async fn persist_sandbox_assignment(
        &self,
        thread_key: &ThreadKey,
        sandbox_id: &str,
        resource_uid: &str,
        capabilities: &SessionSandboxCapabilities,
        expected_assignment: &SandboxAssignmentSnapshot,
        execution_metadata: Option<&Value>,
    ) -> Result<(), SessionRuntimeError> {
        if !capabilities.metadata_trace_enabled {
            if !self
                .store
                .update_sandbox_assignment_if_matches(
                    thread_key,
                    sandbox_id,
                    Some(resource_uid),
                    capabilities,
                    expected_assignment,
                )
                .await?
            {
                return Err(SessionRuntimeError::SandboxAssignmentChanged);
            }
            return Ok(());
        }
        let Some(identity) = self.metadata_trace_config.as_ref() else {
            return Err(SessionRuntimeError::InactiveMetadataTraceConfig);
        };
        let Some(subject) =
            SlackTraceSubject::from_execution_metadata(thread_key.as_str(), execution_metadata)
        else {
            return Err(SessionRuntimeError::MetadataTraceBoundaryChanged);
        };
        if !self
            .store
            .update_sandbox_assignment_if_metadata_trace_config_active(
                thread_key,
                sandbox_id,
                capabilities,
                identity,
                expected_assignment,
                subject.workspace_id(),
                subject.user_id(),
                resource_uid,
            )
            .await?
        {
            return Err(SessionRuntimeError::InactiveMetadataTraceConfig);
        }
        Ok(())
    }

    async fn record_sandbox_ready(&self, observation: SandboxReadyObservation<'_>) {
        let SandboxReadyObservation {
            thread_key,
            execution_id,
            sandbox_id,
            harness_type,
            source,
            ready_duration,
            startup_duration,
        } = observation;
        let ready_duration_ms = duration_millis_u64(ready_duration);
        let startup_duration_ms = startup_duration.map(duration_millis_u64).unwrap_or(0);
        let sandbox_started_for_request = startup_duration.is_some();

        if let Err(error) = self
            .store
            .touch_sandbox_activity(thread_key, sandbox_id)
            .await
        {
            warn!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "session_sandbox_activity_touch_failed",
                thread_key = %thread_key,
                execution_id,
                sandbox_id,
                %error,
                "failed to touch sandbox activity after sandbox ready"
            );
        }

        if let Err(error) = self
            .store
            .append_event(
                thread_key,
                Some(execution_id),
                "session.sandbox_ready",
                json!({
                    "execution_id": execution_id,
                    "thread_key": thread_key.as_str(),
                    "sandbox_id": sandbox_id,
                    "harness_type": harness_type.to_string(),
                    "sandbox_ready_source": source,
                    "sandbox_ready_duration_ms": ready_duration_ms,
                    "sandbox_startup_duration_ms": startup_duration_ms,
                    "sandbox_started_for_request": sandbox_started_for_request,
                }),
            )
            .await
        {
            warn!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "sandbox_ready_event_append_failed",
                thread_key = %thread_key,
                execution_id,
                sandbox_id,
                %error,
                "failed to append sandbox ready event"
            );
        }

        info!(
            component = COMPONENT_SESSION_RUNTIME,
            event = "sandbox_ready",
            thread_key = %thread_key,
            execution_id,
            sandbox_id,
            harness_type = %harness_type,
            sandbox_ready_source = source,
            sandbox_ready_duration_ms = ready_duration_ms,
            sandbox_startup_duration_ms = startup_duration_ms,
            sandbox_started_for_request,
            "session sandbox ready"
        );
    }

    async fn ensure_session_pipe_if_live(
        &self,
        thread_key: &ThreadKey,
        sandbox_id: &str,
    ) -> Result<(), SessionRuntimeError> {
        let id = SandboxId::new(sandbox_id);
        match self.sandbox_runtime.manager.status(&id).await {
            Ok(status) if should_attach_session_pipe(&status) => {
                if let Err(error) = self.ensure_session_pipe(thread_key, sandbox_id).await
                    && !is_event_stream_attach_race(&error)
                {
                    return Err(error);
                }
            }
            Ok(_) => {}
            Err(SandboxError::NotFound(_)) => {}
            Err(error) => return Err(SessionRuntimeError::Sandbox(error)),
        }
        Ok(())
    }

    async fn ensure_session_pipe(
        &self,
        thread_key: &ThreadKey,
        sandbox_id: &str,
    ) -> Result<SessionPipe, SessionRuntimeError> {
        self.ensure_session_pipe_with_output_state(
            thread_key,
            sandbox_id,
            StdoutPumpState::default(),
        )
        .await
    }

    fn sandbox_pipe_open_lock(&self, sandbox_id: &str) -> SessionPipeOpenLock {
        registered_lock_from_registry(&self.sandbox_pipe_open_locks, sandbox_id, || Mutex::new(()))
    }

    fn sandbox_output_gate(&self, sandbox_id: &str) -> SessionOutputGate {
        output_gate_from_registry(&self.sandbox_output_gates, sandbox_id)
    }

    async fn persist_adopted_root_thread_id(
        &self,
        thread_key: &ThreadKey,
        execution_id: &str,
        root_thread_id: &str,
    ) -> Result<Option<Session>, SessionStoreError> {
        #[cfg(test)]
        if self
            .fail_adoption_root_persistence
            .swap(false, Ordering::SeqCst)
        {
            return Err(SessionStoreError::InvalidPersistedValue(
                "injected adoption root persistence failure".to_owned(),
            ));
        }
        self.store
            .update_harness_thread_id_if_stdout_owner(
                thread_key,
                execution_id,
                &self.stdout_owner_id,
                Some(root_thread_id),
            )
            .await
    }

    async fn ensure_session_pipe_with_output_state(
        &self,
        thread_key: &ThreadKey,
        sandbox_id: &str,
        output_state: StdoutPumpState,
    ) -> Result<SessionPipe, SessionRuntimeError> {
        let span = info_span!(
            "centaur.api_rs.session.pipe.ensure",
            component = COMPONENT_SESSION_RUNTIME,
            event = "session_pipe_ensure",
            "centaur.thread_key" = thread_key.as_str(),
            "centaur.sandbox_id" = sandbox_id,
            thread_key = %thread_key,
            sandbox_id,
        );
        let result = async {
            let output_gate = self.sandbox_output_gate(sandbox_id);
            let open_lock = self.sandbox_pipe_open_lock(sandbox_id);
            let _open_guard = open_lock.lock().await;

            let output_state = if let Some(pipe) = self
                .sandbox_pipes
                .get(sandbox_id)
                .map(|entry| entry.clone())
            {
                pipe.output_state.lock().await.merge_from(output_state);
                if pipe.stdout_alive.load(Ordering::Acquire) {
                    info!(
                        component = COMPONENT_SESSION_RUNTIME,
                        event = "session_pipe_reused",
                        thread_key = %thread_key,
                        sandbox_id,
                        "reusing session pipe"
                    );
                    return Ok(pipe);
                }
                pipe.output_state.clone()
            } else {
                Arc::new(Mutex::new(output_state))
            };

            let io = self
                .sandbox_runtime
                .manager
                .open_io(&SandboxId::new(sandbox_id))
                .await?
                .into_parts();
            let attached_resource_uid = io.resource_uid.clone();
            let mut pipe = session_pipe_from_stdin(io.stdin, output_state.clone(), output_gate);
            let assignment = self
                .store
                .ensure_current_sandbox_assignment_identity(
                    thread_key,
                    sandbox_id,
                    attached_resource_uid.as_deref(),
                )
                .await?
                .ok_or(SessionRuntimeError::SandboxAssignmentChanged)?;
            pipe.assignment_epoch = Some(assignment.assignment_epoch);
            pipe.resource_uid = assignment.resource_uid;
            if let Some(assignment) = self
                .store
                .metadata_trace_assignment_actor(thread_key, sandbox_id)
                .await?
            {
                if attached_resource_uid.as_deref() != Some(assignment.resource_uid.as_str()) {
                    return Err(SessionRuntimeError::MetadataTraceBoundaryChanged);
                }
                pipe.trace_assignment_epoch = Some(assignment.assignment_epoch);
                pipe.trace_resource_uid = Some(assignment.resource_uid);
            }

            self.sandbox_pipes
                .insert(sandbox_id.to_owned(), pipe.clone());
            drop(_open_guard);
            let ctx = self.context();
            let thread_key = thread_key.clone();
            let pump_thread_key = thread_key.clone();
            let pump_key = sandbox_id.to_owned();
            let pump_pipe = pipe.clone();
            let stdout = io.stdout;
            let stderr = io.stderr;
            let guard = io.guard;
            let stderr_key = pump_key.clone();

            spawn_stdout_pump_loop(StdoutPumpLoop {
                ctx,
                open_lock,
                thread_key: pump_thread_key,
                sandbox_id: pump_key,
                pipe: pump_pipe,
                stdout,
                guard,
            });

            spawn_stderr_drain(stderr_key, stderr);

            info!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "session_pipe_opened",
                thread_key = %thread_key,
                sandbox_id,
                "session pipe opened"
            );
            Ok(pipe)
        }
        .instrument(span.clone())
        .await;

        if let Err(error) = &result {
            error!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "session_pipe_ensure_failed",
                thread_key = %thread_key,
                sandbox_id,
                %error,
                "failed to ensure session pipe"
            );
        }
        result
    }

    /// Reconciles executions left `queued`/`running` by a previous control
    /// plane process. Execution rows never time out on their own: the only
    /// writer of a terminal status is the process that was watching the
    /// sandbox, so a kill mid-turn leaves the row active forever, wedging the
    /// thread (the one-active-execution index blocks new executes) and any
    /// event-stream consumer waiting for a terminal event.
    ///
    /// Adoption order of preference:
    /// 1. The sandbox already finished the turn while nobody was attached:
    ///    recover the terminal outcome from the backend's recorded output.
    /// 2. The sandbox is still running the turn: re-attach the stdout pump
    ///    and re-arm the remaining max-duration deadline.
    /// 3. The sandbox is gone: record the failure honestly.
    pub async fn adopt_orphaned_executions(&self) {
        // A one-shot scan has no later tick to revisit skipped rows, so
        // queued orphans are failed immediately regardless of age — the
        // pre-rescan startup behavior.
        self.run_orphan_adoption_scan(&mut OrphanAdoptionState::default(), None)
            .await;
    }

    /// Re-run the orphan adoption scan every `interval` for the lifetime of
    /// the process (the first scan runs immediately). A startup-only scan
    /// misses executions orphaned after it ran — most commonly the previous
    /// pod of a rolling deploy reaching its termination grace period
    /// mid-turn after the new pod already scanned — and those stay wedged
    /// until the next deploy.
    pub fn spawn_orphan_adoption(&self, interval: Duration) {
        let runtime = self.clone();
        tokio::spawn(async move {
            let mut state = OrphanAdoptionState::default();
            let mut ticker = interval_at(Instant::now(), interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                runtime
                    .run_orphan_adoption_scan(&mut state, Some(PRE_SANDBOX_ORPHAN_GRACE))
                    .await;
            }
        });
    }

    /// One pass over all active executions. `pre_sandbox_grace` is the
    /// minimum age before a row awaiting sandbox assignment is treated as
    /// orphaned; `None` is only correct when no re-scan will follow.
    async fn run_orphan_adoption_scan(
        &self,
        state: &mut OrphanAdoptionState,
        pre_sandbox_grace: Option<Duration>,
    ) {
        if self.shutting_down.load(Ordering::SeqCst) {
            state.deferred.clear();
            return;
        }
        let recoverable_deliveries = match self.store.list_recoverable_input_deliveries().await {
            Ok(deliveries) => deliveries,
            Err(error) => {
                warn!(%error, "input-delivery recovery scan failed");
                return;
            }
        };
        for delivery in recoverable_deliveries {
            if self.shutting_down.load(Ordering::SeqCst) {
                return;
            }
            let claim = match self
                .claim_input_delivery(Some(&delivery.execution_id), Some(&delivery.delivery_id))
                .await
            {
                Ok(Some(claim)) => claim,
                Ok(None) => continue,
                Err(error) => {
                    warn!(delivery_id = %delivery.delivery_id, %error, "input-delivery claim failed");
                    continue;
                }
            };
            if let Err(error) = self.drive_claimed_input_delivery(&claim).await {
                self.finish_failed_input_delivery(&claim, &error).await;
                warn!(
                    delivery_id = %claim.delivery.delivery_id,
                    execution_id = %claim.execution.execution_id,
                    %error,
                    "input-delivery recovery attempt failed"
                );
            } else {
                // A recovered delivery claims the stdout lease before this
                // scan reaches active-execution adoption. That makes the
                // later self-owner fast path intentionally skip it, so arm
                // the durable execution deadline here rather than leaving a
                // silent recovered turn unbounded until ownership changes.
                spawn_remaining_max_duration_failure(self.context(), &claim.execution);
            }
        }
        let executions = match self.store.list_active_executions_with_ownership().await {
            Ok(executions) => executions,
            Err(error) => {
                warn!(
                    component = COMPONENT_SESSION_RUNTIME,
                    event = "execution_adoption_scan_failed",
                    %error,
                    "failed to list orphaned executions"
                );
                return;
            }
        };
        if executions.is_empty() {
            state.deferred.clear();
            return;
        }
        let mut adopted = 0_usize;
        let mut failed = 0_usize;
        let mut skipped = 0_usize;
        let mut own = 0_usize;
        let mut deferred = HashSet::new();
        for candidate in executions {
            let execution_id = candidate.execution.execution_id.clone();
            // Advisory fast path: a live lease means the execution has an
            // active pump somewhere. Skip our own executions silently and
            // defer peers' without touching the session row or the sandbox
            // backend — the conditional claim below stays the sole authority
            // on ownership.
            if candidate.stdout_owner_lease_active {
                if candidate.stdout_owner_id.as_deref() == Some(self.stdout_owner_id.as_str()) {
                    own += 1;
                    continue;
                }
                if !state.deferred.contains(&execution_id) {
                    self.record_adoption_deferral(&candidate.execution).await;
                }
                deferred.insert(execution_id);
                continue;
            }
            let record_deferral = !state.deferred.contains(&execution_id);
            match self
                .adopt_orphaned_execution(&candidate.execution, record_deferral, pre_sandbox_grace)
                .await
            {
                Ok(OrphanAdoption::Adopted) => adopted += 1,
                Ok(OrphanAdoption::Failed) => failed += 1,
                Ok(OrphanAdoption::Skipped) => skipped += 1,
                Ok(OrphanAdoption::Deferred) => {
                    deferred.insert(execution_id);
                }
                Err(error) => {
                    warn!(
                        component = COMPONENT_SESSION_RUNTIME,
                        event = "execution_adoption_failed",
                        thread_key = %candidate.execution.thread_key,
                        execution_id = %candidate.execution.execution_id,
                        %error,
                        "failed to adopt orphaned execution; will retry on the next scan"
                    );
                    // Keep the dedup entry across transient errors so a
                    // recovered deferral is not re-recorded.
                    if state.deferred.contains(&execution_id) {
                        deferred.insert(execution_id);
                    }
                }
            }
        }
        state.deferred = deferred;
        if adopted > 0 || failed > 0 {
            info!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "execution_adoption_scan",
                adopted,
                failed,
                deferred = state.deferred.len(),
                skipped,
                own,
                "adopted executions orphaned by a previous control plane process"
            );
        } else {
            debug!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "execution_adoption_scan",
                adopted,
                failed,
                deferred = state.deferred.len(),
                skipped,
                own,
                "orphan adoption scan found nothing adoptable"
            );
        }
    }

    async fn record_adoption_deferral(&self, execution: &SessionExecution) {
        info!(
            component = COMPONENT_SESSION_RUNTIME,
            event = "execution_adoption_deferred",
            thread_key = %execution.thread_key,
            execution_id = %execution.execution_id,
            "active stdout owner lease still exists; deferring adoption"
        );
        let _ = self
            .store
            .append_event(
                &execution.thread_key,
                Some(&execution.execution_id),
                "session.execution_adoption_deferred",
                json!({ "reason": "stdout_owner_lease_active" }),
            )
            .await;
    }

    async fn adopt_orphaned_execution(
        &self,
        execution: &SessionExecution,
        record_deferral: bool,
        pre_sandbox_grace: Option<Duration>,
    ) -> Result<OrphanAdoption, SessionRuntimeError> {
        if self.shutting_down.load(Ordering::SeqCst) {
            return Err(SessionRuntimeError::ShuttingDown);
        }
        let thread_key = &execution.thread_key;
        let execution_id = execution.execution_id.as_str();
        if execution.status == ExecutionStatus::Queued {
            // Input is only written after an execution is marked running, so
            // a queued orphan never reached the harness: nothing can come.
            // On a periodic scan, young queued rows are skipped instead of
            // failed: they are most likely a live execute_session observed
            // mid-transition, and a later tick revisits them.
            if let Some(grace) = pre_sandbox_grace {
                let age = SystemTime::now()
                    .duration_since(SystemTime::from(execution.created_at))
                    .unwrap_or_default();
                if age < grace {
                    debug!(
                        component = COMPONENT_SESSION_RUNTIME,
                        event = "execution_adoption_skipped",
                        thread_key = %thread_key,
                        execution_id,
                        age_ms = duration_millis_u64(age),
                        "skipping young queued execution; a live execute may still claim it"
                    );
                    return Ok(OrphanAdoption::Skipped);
                }
            }
            self.fail_orphaned_execution(
                thread_key,
                execution_id,
                "",
                "orphaned before input was sent",
            )
            .await;
            return Ok(OrphanAdoption::Failed);
        }
        let session = self.store.get_session(thread_key).await?;
        let Some(sandbox_id) = session.sandbox_id.as_deref() else {
            let running_since = execution.started_at.unwrap_or(execution.created_at);
            let running_age = SystemTime::now()
                .duration_since(SystemTime::from(running_since))
                .unwrap_or_default();
            if pre_sandbox_grace.is_some_and(|grace| running_age < grace) {
                debug!(
                    component = COMPONENT_SESSION_RUNTIME,
                    event = "execution_adoption_skipped",
                    thread_key = %thread_key,
                    execution_id,
                    age_ms = duration_millis_u64(running_age),
                    "skipping young running execution awaiting sandbox assignment"
                );
                return Ok(OrphanAdoption::Skipped);
            }
            self.fail_orphaned_execution(
                thread_key,
                execution_id,
                "",
                "orphaned with no sandbox assigned",
            )
            .await;
            return Ok(OrphanAdoption::Failed);
        };
        let id = SandboxId::new(sandbox_id);
        let status = match self.sandbox_runtime.manager.status(&id).await {
            Ok(status) => status,
            Err(SandboxError::NotFound(_)) => SandboxStatus::Gone,
            // Transient status failures must not fail a possibly live
            // execution; surface the error and retry on the next startup.
            Err(error) => return Err(SessionRuntimeError::Sandbox(error)),
        };
        if !status.can_open_io() {
            self.fail_orphaned_execution(
                thread_key,
                execution_id,
                sandbox_id,
                &format!("sandbox no longer accepts io (status {status:?})"),
            )
            .await;
            return Ok(OrphanAdoption::Failed);
        }
        // An existing pump may still be attached while its previous owner
        // lease is expired. Fence its line processing before claiming the
        // execution, then keep the fence through recorded replay and state
        // merge so child output cannot terminalize against an unseeded state.
        let output_gate = self.sandbox_output_gate(sandbox_id);
        let _output_guard = output_gate.write().await;
        if !self.claim_expired_stdout_owner(execution_id).await? {
            // Deferrals repeat on every periodic scan while another control
            // plane pumps the execution; only the first observation is worth
            // an info log and a durable event.
            if record_deferral {
                self.record_adoption_deferral(execution).await;
            } else {
                debug!(
                    component = COMPONENT_SESSION_RUNTIME,
                    event = "execution_adoption_deferred",
                    thread_key = %thread_key,
                    execution_id,
                    sandbox_id,
                    "active stdout owner lease still exists; deferring adoption"
                );
            }
            return Ok(OrphanAdoption::Deferred);
        }

        let since = execution.started_at.unwrap_or(execution.created_at);
        spawn_remaining_max_duration_failure(self.context(), execution);
        let adoption_io_deadline = Instant::now() + EXECUTION_ADOPTION_IO_TIMEOUT;

        // The turn may have finished while no control plane was attached. An
        // attach stream cannot replay that output, but the backend's recorded
        // history (pod logs) can.
        let lines = match timeout_at(
            adoption_io_deadline,
            self.sandbox_runtime
                .manager
                .read_output_since(&id, Some(SystemTime::from(since))),
        )
        .await
        {
            Ok(Ok(lines)) => lines,
            Ok(Err(SandboxError::Unsupported { .. })) => Vec::new(),
            Ok(Err(error)) => {
                warn!(
                    component = COMPONENT_SESSION_RUNTIME,
                    event = "execution_adoption_log_read_failed",
                    thread_key = %thread_key,
                    execution_id,
                    sandbox_id,
                    %error,
                    "failed to read recorded sandbox output; adopting live"
                );
                Vec::new()
            }
            Err(_) => {
                warn!(
                    component = COMPONENT_SESSION_RUNTIME,
                    event = "execution_adoption_log_read_timed_out",
                    thread_key = %thread_key,
                    execution_id,
                    sandbox_id,
                    timeout_ms = duration_millis_u64(EXECUTION_ADOPTION_IO_TIMEOUT),
                    "recorded sandbox output read timed out; releasing adoption ownership for retry"
                );
                self.abandon_stdout_owner(execution_id).await;
                return Ok(OrphanAdoption::Deferred);
            }
        };
        let mut output_state = stdout_state_for_execution(&session, execution_id);
        if session.harness_type != HarnessType::Nanocodex
            && let Some(durable_root) = session.harness_thread_id.as_deref()
            && let Some(repaired_root) =
                legacy_corrupted_root_repair_candidate(&lines, durable_root)
        {
            info!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "execution_adoption_root_repaired",
                thread_key = %thread_key,
                execution_id,
                sandbox_id,
                durable_root,
                repaired_root,
                "repairing a legacy child-corrupted harness thread id from recorded execution evidence"
            );
            output_state.set_authoritative_root_thread_id(execution_id, &repaired_root);
        }
        let terminal = output_state.replay_recorded_output(execution_id, &lines);
        if let Some(root_thread_id) = output_state.root_thread_id(execution_id) {
            match self
                .persist_adopted_root_thread_id(thread_key, execution_id, root_thread_id)
                .await
            {
                Ok(Some(_)) => {}
                Ok(None) => {
                    warn!(
                        component = COMPONENT_SESSION_RUNTIME,
                        event = "execution_adoption_stdout_owner_lost",
                        thread_key = %thread_key,
                        execution_id,
                        sandbox_id,
                        "adoption lost stdout ownership before root persistence; deferring without attaching"
                    );
                    self.abandon_stdout_owner(execution_id).await;
                    return Ok(OrphanAdoption::Deferred);
                }
                Err(error) => {
                    warn!(
                        component = COMPONENT_SESSION_RUNTIME,
                        event = "execution_adoption_thread_id_persist_failed",
                        thread_key = %thread_key,
                        execution_id,
                        sandbox_id,
                        %error,
                        "failed to persist root harness thread id recovered during adoption; releasing ownership for retry"
                    );
                    self.abandon_stdout_owner(execution_id).await;
                    return Err(SessionRuntimeError::Store(error));
                }
            }
        }
        if let Some(terminal) = terminal {
            if !record_terminal_output(
                &self.context(),
                thread_key,
                sandbox_id,
                execution_id,
                terminal,
            )
            .await?
            {
                warn!(
                    component = COMPONENT_SESSION_RUNTIME,
                    event = "execution_adoption_stdout_owner_lost",
                    thread_key = %thread_key,
                    execution_id,
                    sandbox_id,
                    "adoption lost stdout ownership before recorded terminal persistence"
                );
                self.abandon_stdout_owner(execution_id).await;
                return Ok(OrphanAdoption::Deferred);
            }
            info!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "execution_adopted",
                thread_key = %thread_key,
                execution_id,
                sandbox_id,
                mode = "recorded_output",
                "adopted orphaned execution from recorded sandbox output"
            );
            let _ = self
                .store
                .append_event(
                    thread_key,
                    Some(execution_id),
                    "session.execution_adopted",
                    json!({ "sandbox_id": sandbox_id, "mode": "recorded_output" }),
                )
                .await;
            return Ok(OrphanAdoption::Adopted);
        }

        // No terminal in the recorded output: treat the turn as still in
        // flight. Re-attach the stdout pump and re-arm the remaining
        // max-duration budget so an adopted-but-silent turn stays bounded.
        match timeout_at(
            adoption_io_deadline,
            self.ensure_session_pipe_with_output_state(thread_key, sandbox_id, output_state),
        )
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                self.abandon_stdout_owner(execution_id).await;
                return Err(error);
            }
            Err(_) => {
                warn!(
                    component = COMPONENT_SESSION_RUNTIME,
                    event = "execution_adoption_attach_timed_out",
                    thread_key = %thread_key,
                    execution_id,
                    sandbox_id,
                    timeout_ms = duration_millis_u64(EXECUTION_ADOPTION_IO_TIMEOUT),
                    "sandbox attach timed out; releasing adoption ownership for retry"
                );
                self.abandon_stdout_owner(execution_id).await;
                return Ok(OrphanAdoption::Deferred);
            }
        }
        info!(
            component = COMPONENT_SESSION_RUNTIME,
            event = "execution_adopted",
            thread_key = %thread_key,
            execution_id,
            sandbox_id,
            mode = "live_attach",
            "adopted orphaned execution with a live sandbox attach"
        );
        let _ = self
            .store
            .append_event(
                thread_key,
                Some(execution_id),
                "session.execution_adopted",
                json!({ "sandbox_id": sandbox_id, "mode": "live_attach" }),
            )
            .await;
        Ok(OrphanAdoption::Adopted)
    }

    async fn fail_orphaned_execution(
        &self,
        thread_key: &ThreadKey,
        execution_id: &str,
        sandbox_id: &str,
        detail: &str,
    ) {
        if self.shutting_down.load(Ordering::SeqCst) {
            return;
        }
        let claimed = self.claim_expired_stdout_owner(execution_id).await;
        match claimed {
            Ok(true) => {}
            Ok(false) => {
                warn!(
                    component = COMPONENT_SESSION_RUNTIME,
                    event = "execution_adoption_fail_owner_lost",
                    thread_key = %thread_key,
                    execution_id,
                    sandbox_id,
                    "orphan failure lost stdout ownership before terminal persistence"
                );
                return;
            }
            Err(error) => {
                warn!(
                    component = COMPONENT_SESSION_RUNTIME,
                    event = "execution_adoption_fail_claim_failed",
                    thread_key = %thread_key,
                    execution_id,
                    sandbox_id,
                    %error,
                    "failed to claim orphaned execution for terminal persistence"
                );
                return;
            }
        }
        let error = format!("execution orphaned by control plane restart; {detail}");
        match record_terminal_output(
            &self.context(),
            thread_key,
            sandbox_id,
            execution_id,
            TerminalOutput::Failed { error },
        )
        .await
        {
            Ok(true) => {}
            Ok(false) => warn!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "execution_adoption_fail_owner_lost",
                thread_key = %thread_key,
                execution_id,
                sandbox_id,
                "orphan failure lost stdout ownership before terminal persistence"
            ),
            Err(record_error) => warn!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "execution_adoption_fail_record_failed",
                thread_key = %thread_key,
                execution_id,
                error = %record_error,
                "failed to record orphaned execution failure"
            ),
        }
    }

    /// Hands off this control plane's in-flight executions before process
    /// exit. Waits up to `timeout` for owned executions to finish naturally
    /// (their stdout pumps keep running until the process exits), then
    /// releases the remaining stdout-owner leases so another control
    /// plane's adoption scan can claim the executions right away instead of
    /// waiting out the lease TTL. Turn output produced after the release is
    /// not lost: adoption replays it from the sandbox backend's recorded
    /// output.
    pub async fn handoff_owned_executions(&self, timeout: Duration) {
        // Fence new stdout-owner claims first: an execution accepted after
        // this point would otherwise claim a lease that outlives the
        // process, stranding it until the lease TTL expires.
        self.shutting_down.store(true, Ordering::SeqCst);
        let _claim_guard = self.stdout_owner_claim_gate.lock().await;
        let deadline = Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(3600));
        loop {
            let count = tokio::time::timeout(
                EXECUTION_HANDOFF_DB_TIMEOUT,
                self.store
                    .count_executions_with_stdout_owner(&self.stdout_owner_id),
            )
            .await;
            let Ok(count) = count else {
                warn!(
                    component = COMPONENT_SESSION_RUNTIME,
                    event = "execution_handoff_count_timeout",
                    "timed out counting in-flight executions; releasing leases now"
                );
                break;
            };
            match count {
                Ok(0) => {
                    if !stop_all_stdout_owner_renewers(&self.stdout_owner_renewals).await {
                        warn!(
                            component = COMPONENT_SESSION_RUNTIME,
                            event = "execution_handoff_renewer_stop_timeout",
                            "timed out stopping a stdout-owner renewer during idle shutdown"
                        );
                    }
                    info!(
                        component = COMPONENT_SESSION_RUNTIME,
                        event = "execution_handoff_idle",
                        "no in-flight executions to hand off at shutdown"
                    );
                    return;
                }
                Ok(in_flight) => {
                    if Instant::now() >= deadline {
                        break;
                    }
                    info!(
                        component = COMPONENT_SESSION_RUNTIME,
                        event = "execution_handoff_waiting",
                        in_flight,
                        "waiting for in-flight executions to finish before shutdown"
                    );
                }
                Err(error) => {
                    warn!(
                        component = COMPONENT_SESSION_RUNTIME,
                        event = "execution_handoff_count_failed",
                        %error,
                        "failed to count in-flight executions; releasing leases now"
                    );
                    break;
                }
            }
            sleep(EXECUTION_HANDOFF_POLL_INTERVAL).await;
        }
        if !stop_all_stdout_owner_renewers(&self.stdout_owner_renewals).await {
            warn!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "execution_handoff_renewer_stop_timeout",
                "timed out stopping a stdout-owner renewer; leaving leases fenced"
            );
            return;
        }
        let released = tokio::time::timeout(
            EXECUTION_HANDOFF_DB_TIMEOUT,
            self.store
                .release_stdout_owned_executions(&self.stdout_owner_id),
        )
        .await;
        let Ok(released) = released else {
            warn!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "execution_handoff_release_timeout",
                "timed out releasing stdout-owner leases; peers must wait for lease expiry"
            );
            return;
        };
        match released {
            Ok(released) => {
                for execution in &released {
                    info!(
                        component = COMPONENT_SESSION_RUNTIME,
                        event = "execution_handoff_released",
                        thread_key = %execution.thread_key,
                        execution_id = %execution.execution_id,
                        "released stdout-owner lease at shutdown for adoption by a peer"
                    );
                    let _ = self
                        .store
                        .append_event(
                            &execution.thread_key,
                            Some(&execution.execution_id),
                            "session.stdout_owner_released",
                            json!({
                                "execution_id": execution.execution_id,
                                "reason": "control_plane_shutdown",
                            }),
                        )
                        .await;
                }
                if released.is_empty() {
                    info!(
                        component = COMPONENT_SESSION_RUNTIME,
                        event = "execution_handoff_idle",
                        "in-flight executions finished during the shutdown drain"
                    );
                }
            }
            Err(error) => {
                warn!(
                    component = COMPONENT_SESSION_RUNTIME,
                    event = "execution_handoff_release_failed",
                    %error,
                    "failed to release stdout-owner leases at shutdown"
                );
            }
        }
    }
}

/// Outcome of one orphan-adoption attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OrphanAdoption {
    /// Terminal output was recovered or a live pump was re-attached.
    Adopted,
    /// Another control plane still holds the stdout-owner lease.
    Deferred,
    /// The execution was failed as unrecoverable.
    Failed,
    /// Too young to judge (freshly queued); revisit on a later scan.
    Skipped,
}

/// Scan state carried across periodic orphan-adoption ticks.
#[derive(Debug, Default)]
struct OrphanAdoptionState {
    /// Executions whose deferral was already recorded, so long-lived leases
    /// do not produce a `session.execution_adoption_deferred` event on every
    /// tick.
    deferred: HashSet<String>,
}

async fn maybe_generate_session_title(
    store: PgSessionStore,
    generator: SessionTitleGenerator,
    thread_key: ThreadKey,
) {
    let parts = match store.title_generation_candidate(&thread_key).await {
        Ok(Some(parts)) => parts,
        Ok(None) => return,
        Err(error) => {
            warn!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "session_title_candidate_failed",
                thread_key = %thread_key,
                %error,
                "failed to load session title candidate"
            );
            return;
        }
    };
    let Some(source) = session_title_source_from_parts(&parts) else {
        return;
    };
    let raw_title = match generator(source).await {
        Ok(title) => title,
        Err(error) => {
            warn!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "session_title_generation_failed",
                thread_key = %thread_key,
                %error,
                "failed to generate session title"
            );
            return;
        }
    };
    let Some(title) = sanitize_session_title(&raw_title) else {
        warn!(
            component = COMPONENT_SESSION_RUNTIME,
            event = "session_title_generation_empty",
            thread_key = %thread_key,
            "session title generation returned an empty title"
        );
        return;
    };
    match store.set_session_title_if_empty(&thread_key, &title).await {
        Ok(true) => {
            info!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "session_title_set",
                thread_key = %thread_key,
                title,
                "session title set"
            );
        }
        Ok(false) => {}
        Err(error) => {
            warn!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "session_title_set_failed",
                thread_key = %thread_key,
                %error,
                "failed to set session title"
            );
        }
    }
}

impl SandboxRuntime {
    pub async fn create_running_io(
        &self,
        spec: SandboxSpec,
    ) -> Result<(SandboxHandle, centaur_sandbox_core::SandboxIoParts), SessionRuntimeError> {
        let handle = self.manager.create_running(spec).await?;
        let io = self.manager.open_io(&handle.id).await?.into_parts();
        Ok((handle, io))
    }

    /// Stop precisely the resource created for a short-lived workflow host.
    ///
    /// Backend names may be reused after a host exits. The create handle's
    /// resource UID prevents a delayed cleanup from stopping its replacement.
    pub async fn stop_sandbox(&self, handle: &SandboxHandle) -> Result<(), SessionRuntimeError> {
        let Some(resource_uid) = handle.resource_uid.as_deref() else {
            return Err(SessionRuntimeError::Sandbox(SandboxError::backend(
                "workflow sandbox create did not return a stable resource UID",
            )));
        };
        self.manager
            .stop_exact(&handle.id, Some(resource_uid))
            .await?;
        Ok(())
    }

    pub fn backend(backend: Arc<dyn SandboxBackend>, spec: SandboxSpec) -> Self {
        let warm_spec = spec.clone();
        let spec_factory =
            move |_thread_key: &ThreadKey,
                  _execution_id: &str,
                  _harness: &HarnessType,
                  _persona: Option<&PersonaContext>| { spec.clone() };
        let warm_spec_factory = move || warm_spec.clone();
        Self::backend_with_warm_spec_factory(backend, spec_factory, warm_spec_factory)
    }

    pub fn backend_with_workload(
        backend: Arc<dyn SandboxBackend>,
        workload: SandboxWorkloadMode,
    ) -> Self {
        let warm_harness = workload.default_harness();
        let warm_workload = workload.clone();
        let mut runtime = Self::backend_with_warm_spec_factory(
            backend,
            move |thread_key, _execution_id, harness, persona| {
                workload.spec(thread_key, harness, persona)
            },
            move || warm_workload.warm_spec(),
        );
        runtime.warm_harness = warm_harness;
        runtime
    }

    pub fn backend_with_spec_factory<F>(backend: Arc<dyn SandboxBackend>, spec_factory: F) -> Self
    where
        F: Fn(&ThreadKey, &str, &HarnessType, Option<&PersonaContext>) -> SandboxSpec
            + Send
            + Sync
            + 'static,
    {
        Self {
            manager: Arc::new(SandboxManager::new(backend)),
            spec_factory: Arc::new(spec_factory),
            warm_spec_factory: None,
            workload_key: None,
            warm_harness: None,
        }
    }

    pub fn backend_with_warm_spec_factory<F, W>(
        backend: Arc<dyn SandboxBackend>,
        spec_factory: F,
        warm_spec_factory: W,
    ) -> Self
    where
        F: Fn(&ThreadKey, &str, &HarnessType, Option<&PersonaContext>) -> SandboxSpec
            + Send
            + Sync
            + 'static,
        W: Fn() -> SandboxSpec + Send + Sync + 'static,
    {
        let warm_spec_factory: WarmSandboxSpecFactory = Arc::new(warm_spec_factory);
        let workload_key = sandbox_spec_key(&warm_spec_factory());
        Self {
            manager: Arc::new(SandboxManager::new(backend)),
            spec_factory: Arc::new(spec_factory),
            warm_spec_factory: Some(warm_spec_factory),
            workload_key: Some(workload_key),
            warm_harness: None,
        }
    }
}

impl SandboxWorkloadMode {
    pub fn mock_app_server(image: impl Into<String>) -> Self {
        Self::MockAppServer {
            image: image.into(),
        }
    }

    pub fn codex_app_server(
        image: impl Into<String>,
        env: impl IntoIterator<Item = (String, String)>,
        harness: HarnessType,
    ) -> Self {
        Self::CodexAppServer {
            image: image.into(),
            env: env.into_iter().collect(),
            mounts: Vec::new(),
            harness,
        }
    }

    pub fn mount(mut self, mount: Mount) -> Self {
        match &mut self {
            Self::MockAppServer { .. } => {}
            Self::CodexAppServer { mounts, .. } => mounts.push(mount),
        }
        self
    }

    fn default_harness(&self) -> Option<HarnessType> {
        match self {
            Self::MockAppServer { .. } => None,
            Self::CodexAppServer { harness, .. } => Some(harness.clone()),
        }
    }

    fn spec(
        &self,
        thread_key: &ThreadKey,
        harness: &HarnessType,
        persona: Option<&PersonaContext>,
    ) -> SandboxSpec {
        self.spec_for(Some(thread_key), harness, persona)
    }

    fn warm_spec(&self) -> SandboxSpec {
        match self {
            Self::MockAppServer { .. } => self.spec_for(None, &HarnessType::Codex, None),
            Self::CodexAppServer { harness, .. } => self.spec_for(None, harness, None),
        }
    }

    fn spec_for(
        &self,
        thread_key: Option<&ThreadKey>,
        harness: &HarnessType,
        persona: Option<&PersonaContext>,
    ) -> SandboxSpec {
        match self {
            Self::MockAppServer { image } => apply_persona_spec_env(
                SandboxSpec::new(image)
                    .command(["/bin/sh", "-lc"])
                    .args([mock_app_server_script()])
                    .env("CENTAUR_HARNESS_TYPE", harness.as_ref()),
                persona,
            ),
            Self::CodexAppServer {
                image, env, mounts, ..
            } => {
                // Pin the harness via container args (the image entrypoint is
                // kept) so the sandbox runs the session's harness rather than
                // whatever the image CMD defaults to.
                let mut spec = SandboxSpec::new(image)
                    .label("centaur.ai/component", "session-sandbox")
                    .label("centaur.ai/harness", harness.to_string())
                    .args(["harness-server", harness_server_subcommand(harness)]);
                if let Some(thread_key) = thread_key {
                    spec = spec.env("CENTAUR_THREAD_KEY", thread_key.as_str());
                }
                for mount in mounts {
                    spec = spec.mount(mount.clone());
                }
                for (name, value) in env {
                    spec = spec.env(name.clone(), value.clone());
                }
                apply_persona_spec_env(spec, persona)
            }
        }
    }
}

/// The harness-server CLI subcommand for a harness type
/// (see crates/harness-server/src/main.rs).
fn harness_server_subcommand(harness: &HarnessType) -> &'static str {
    match harness {
        HarnessType::Codex => "codex",
        HarnessType::ClaudeCode => "claude-code",
        HarnessType::Amp => "amp",
        HarnessType::Nanocodex => "nanocodex",
    }
}

fn sandbox_spec_key(spec: &SandboxSpec) -> String {
    let encoded = serde_json::to_vec(spec).expect("sandbox specs should serialize");
    let digest = Sha256::digest(encoded);
    format!("sandbox-spec-sha256:{}", hex::encode(digest))
}

fn mock_app_server_script() -> &'static str {
    r#"while IFS= read -r line; do
model="$(printf '%s\n' "$line" | sed -n 's/.*"model":"\([^"]*\)".*/\1/p')"
[ -n "$model" ] || model="unknown"
harness="${CENTAUR_HARNESS_TYPE:-unknown}"
printf '%s\n' '{"type":"system","subtype":"wrapper_heartbeat","phase":"startup"}'
sleep 0.2
printf '%s\n' '{"type":"system","subtype":"wrapper_heartbeat","phase":"app_server_started"}'
sleep 0.2
printf '%s\n' '{"type":"thread.started","thread_id":"mock-codex-thread"}'
sleep 0.2
turn_index=1
while [ "$turn_index" -le 3 ]; do
  turn_id="mock-turn-$turn_index"
  printf '{"type":"turn.started","turn_id":"%s"}\n' "$turn_id"
  sleep 0.2
  printf '{"type":"item.agentMessage.delta","turnId":"%s","session_id":"mock-codex-thread","delta":"PONG model=%s harness=%s"}\n' "$turn_id" "$model" "$harness"
  sleep 0.2
  printf '{"type":"turn.completed","turn":{"id":"%s"},"usage":{"input_tokens":0,"output_tokens":1}}\n' "$turn_id"
  sleep 0.2
  turn_index=$((turn_index + 1))
done
done"#
}

fn session_event_stream(
    store: PgSessionStore,
    thread_key: ThreadKey,
    after_event_id: i64,
    execution_id: Option<String>,
    listener: SessionEventListener,
    span: Span,
) -> impl Stream<Item = Result<SessionEvent, SessionRuntimeError>> {
    stream::unfold(
        EventStreamState {
            store,
            thread_key,
            after_event_id,
            execution_id,
            pending: VecDeque::new(),
            listener,
            safety_tick: {
                let mut tick = interval_at(
                    Instant::now() + EVENT_STREAM_SAFETY_POLL_INTERVAL,
                    EVENT_STREAM_SAFETY_POLL_INTERVAL,
                );
                tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
                tick
            },
            done: false,
            emitted_count: 0,
            span,
        },
        |mut state| {
            let span = state.span.clone();
            async move {
                loop {
                    if let Some(event) = state.pending.pop_front() {
                        state.after_event_id = event.event_id;
                        state.emitted_count += 1;
                        // Execution-scoped streams are per-turn: after the
                        // execution's terminal event nothing else will ever
                        // arrive, so complete the response instead of parking
                        // forever. Abandoned client connections otherwise pin
                        // this stream's dedicated LISTEN connection until the
                        // TCP peer is proven dead (the 2026-07-06 incident
                        // exhausted both the Slackbot fetch pool and staging
                        // Postgres this way). The 30s safety tick makes this
                        // robust even when the notify is missed.
                        if state.execution_id.is_some()
                            && is_terminal_execution_event(&event.event_type)
                        {
                            state.done = true;
                        }
                        return Some((Ok(event), state));
                    }
                    if state.done {
                        info!(
                            component = COMPONENT_SESSION_RUNTIME,
                            event = "session_events_stream_completed",
                            thread_key = %state.thread_key,
                            emitted_count = state.emitted_count,
                            "session event stream completed"
                        );
                        return None;
                    }
                    match state
                        .store
                        .list_events_after(
                            &state.thread_key,
                            state.after_event_id,
                            state.execution_id.as_deref(),
                            100,
                        )
                        .await
                    {
                        Ok(events) if events.is_empty() => loop {
                            tokio::select! {
                                notification = state.listener.recv() => {
                                    match notification {
                                        Ok(notification)
                                            if notification.thread_key == state.thread_key.as_str()
                                                && notification.event_id > state.after_event_id =>
                                        {
                                            break;
                                        }
                                        Ok(_) => {}
                                        Err(error) => {
                                            state.done = true;
                                            return Some((Err(SessionRuntimeError::Store(error)), state));
                                        }
                                    }
                                }
                                _ = state.safety_tick.tick() => break,
                            }
                        }
                        Ok(events) => state.pending = events.into(),
                        Err(error) => {
                            state.done = true;
                            return Some((Err(SessionRuntimeError::Store(error)), state));
                        }
                    }
                }
            }
            .instrument(span)
        },
    )
}

/// Terminal event types for a single execution: once one of these is emitted
/// on an execution-scoped stream, the stream has nothing left to deliver.
fn is_terminal_execution_event(event_type: &str) -> bool {
    matches!(
        event_type,
        "session.execution_completed" | "session.execution_failed" | "session.execution_cancelled"
    )
}

/// How a stdout pump pass ended once the attach stream closed.
enum StdoutPumpEnd {
    /// The stream closed with no execution in flight, or the execution was
    /// already terminalized by a read/codec failure.
    Idle,
    /// The stream closed while an execution was still active. Treat this as a
    /// transport detach; the pump loop decides whether to recover or fail.
    EofActiveExecution {
        execution: Box<SessionExecution>,
        lines_pumped: u64,
    },
}

struct StdoutPumpLoop {
    ctx: RuntimeContext,
    open_lock: SessionPipeOpenLock,
    thread_key: ThreadKey,
    sandbox_id: String,
    pipe: SessionPipe,
    stdout: SandboxRead,
    guard: SandboxIoGuard,
}

enum ReattachOutcome {
    Reattached {
        pipe: SessionPipe,
        stdout: SandboxRead,
        guard: SandboxIoGuard,
    },
    /// Another pipe replaced ours; that pump now owns the sandbox stream.
    Superseded,
    /// A retryable attach/status failure. The caller bounds attempts.
    Retryable(String),
    /// The sandbox cannot serve IO anymore.
    Dead(String),
}

fn stdout_state_for_execution(session: &Session, execution_id: &str) -> StdoutPumpState {
    let mut output_state = StdoutPumpState::default();
    // Codex app-server reuses one durable thread across turns. Nanocodex uses
    // a fresh request id for every run, so its persisted id is observability
    // state only and must never be seeded into a later execution.
    if session.harness_type != HarnessType::Nanocodex
        && let Some(harness_thread_id) = session.harness_thread_id.as_deref()
    {
        output_state.set_authoritative_root_thread_id(execution_id, harness_thread_id);
    }
    output_state
}

fn registered_lock_from_registry<T, F>(
    registry: &WeakLockRegistry<T>,
    key: &str,
    make_lock: F,
) -> SharedRegisteredLock<T>
where
    F: FnOnce() -> T,
{
    match registry.entry(key.to_owned()) {
        dashmap::mapref::entry::Entry::Occupied(mut entry) => {
            if let Some(lock) = entry.get().upgrade() {
                lock
            } else {
                let lock = Arc::new(RegisteredLock {
                    key: key.to_owned(),
                    lock: make_lock(),
                    registry: Arc::downgrade(registry),
                });
                entry.insert(Arc::downgrade(&lock));
                lock
            }
        }
        dashmap::mapref::entry::Entry::Vacant(entry) => {
            let lock = Arc::new(RegisteredLock {
                key: key.to_owned(),
                lock: make_lock(),
                registry: Arc::downgrade(registry),
            });
            entry.insert(Arc::downgrade(&lock));
            lock
        }
    }
}

fn output_gate_from_registry(
    output_gates: &SessionOutputGates,
    sandbox_id: &str,
) -> SessionOutputGate {
    registered_lock_from_registry(output_gates, sandbox_id, || tokio::sync::RwLock::new(()))
}

fn session_pipe_from_stdin(
    stdin: SandboxWrite,
    output_state: SharedStdoutPumpState,
    output_gate: SessionOutputGate,
) -> SessionPipe {
    SessionPipe {
        stdin: Arc::new(Mutex::new(FramedWrite::new(stdin, LinesCodec::new()))),
        output_state,
        output_gate,
        stdout_alive: Arc::new(AtomicBool::new(true)),
        assignment_epoch: None,
        resource_uid: None,
        trace_assignment_epoch: None,
        trace_resource_uid: None,
        #[cfg(test)]
        output_gate_read_wait_started: Arc::new(tokio::sync::Notify::new()),
    }
}

fn spawn_stderr_drain(sandbox_id: String, stderr: SandboxRead) {
    tokio::spawn(async move {
        if let Err(error) = drain_stderr(stderr).await {
            warn!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "session_stderr_drain_failed",
                sandbox_id = %sandbox_id,
                %error,
                "session stderr drain failed"
            );
        }
    });
}

fn remove_pipe_if_current(sandbox_pipes: &SessionPipeMap, sandbox_id: &str, pipe: &SessionPipe) {
    sandbox_pipes.remove_if(sandbox_id, |_sandbox_id, current| {
        Arc::ptr_eq(&current.stdin, &pipe.stdin)
    });
}

async fn stop_exact_and_confirm(
    manager: &SandboxManager,
    sandbox_id: &str,
    resource_uid: &str,
) -> Result<(bool, bool), SessionRuntimeError> {
    let id = SandboxId::new(sandbox_id);
    let mut missing = false;
    let stopped = timeout(
        assignment_reconciliation_backend_timeout(),
        manager.stop_exact(&id, Some(resource_uid)),
    )
    .await
    .map_err(|_| SandboxError::backend("sandbox stop timed out while assignment locked"))?;
    match stopped {
        Ok(()) => {}
        Err(SandboxError::NotFound(_)) => missing = true,
        Err(error) => return Err(SessionRuntimeError::Sandbox(error)),
    }
    match observe_assignment_reconciliation(manager, &id).await {
        Ok(observation) => Ok((
            observation.status == SandboxStatus::Gone
                || observation.resource_uid.as_deref() != Some(resource_uid),
            missing,
        )),
        Err(SandboxError::NotFound(_)) => Ok((true, true)),
        Err(error) => Err(SessionRuntimeError::Sandbox(error)),
    }
}

async fn observe_assignment_reconciliation(
    manager: &SandboxManager,
    id: &SandboxId,
) -> Result<centaur_sandbox_core::ObservedSandbox, SandboxError> {
    timeout(
        assignment_reconciliation_backend_timeout(),
        manager.observe(id),
    )
    .await
    .map_err(|_| SandboxError::backend("sandbox observation timed out while assignment locked"))?
}

async fn pause_assignment_reconciliation(
    manager: &SandboxManager,
    id: &SandboxId,
    resource_uid: &str,
) -> Result<(), SandboxError> {
    timeout(
        assignment_reconciliation_backend_timeout(),
        manager.pause_exact(id, Some(resource_uid)),
    )
    .await
    .map_err(|_| SandboxError::backend("sandbox pause timed out while assignment locked"))?
}

async fn fence_running_assignment(
    store: &PgSessionStore,
    manager: &SandboxManager,
    thread_key: &ThreadKey,
    sandbox_id: &str,
) -> Result<bool, SessionRuntimeError> {
    let Some(assignment_lock) = store
        .lock_sandbox_assignment_for_reconciliation(thread_key, sandbox_id)
        .await?
    else {
        return Ok(false);
    };
    let Some(resource_uid) = assignment_lock.resource_uid().map(str::to_owned) else {
        assignment_lock.rollback().await?;
        return Ok(false);
    };
    let id = SandboxId::new(sandbox_id);
    // This exact running write is the successor-side fence for an earlier
    // timed-out pause whose Kubernetes PATCH might still arrive later.
    let fence = timeout(
        assignment_reconciliation_backend_timeout(),
        manager.ensure_running_exact(&id, &resource_uid, &Uuid::new_v4().to_string()),
    )
    .await
    .map_err(|_| {
        SandboxError::backend("sandbox running fence timed out while assignment locked")
    })?;
    if let Err(error) = fence {
        assignment_lock.rollback().await?;
        return Err(SessionRuntimeError::Sandbox(error));
    }
    let confirmed = timeout(
        assignment_reconciliation_backend_timeout(),
        manager.observe(&id),
    )
    .await
    .ok()
    .and_then(Result::ok)
    .is_some_and(|observation| {
        observation.resource_uid.as_deref() == Some(resource_uid.as_str())
            && observation.status == SandboxStatus::Running
    });
    if !confirmed {
        assignment_lock.rollback().await?;
        return Ok(false);
    }
    assignment_lock
        .commit_if_current()
        .await
        .map_err(Into::into)
}

fn remove_pipe_for_assignment(
    sandbox_pipes: &SessionPipeMap,
    sandbox_id: &str,
    resource_uid: &str,
    assignment_epoch: &str,
) {
    sandbox_pipes.remove_if(sandbox_id, |_sandbox_id, pipe| {
        pipe.resource_uid.as_deref() == Some(resource_uid)
            && pipe.assignment_epoch.as_deref() == Some(assignment_epoch)
    });
}

/// Runs the stdout pump and reattaches when Kubernetes closes the attach
/// stream before the active execution emits terminal output.
fn spawn_stdout_pump_loop(state: StdoutPumpLoop) {
    tokio::spawn(async move {
        let StdoutPumpLoop {
            ctx,
            open_lock,
            thread_key,
            sandbox_id,
            mut pipe,
            mut stdout,
            mut guard,
        } = state;
        let mut reattach_attempts = 0_u32;
        let mut last_reattach_detail = "stdout reattach attempts exhausted".to_owned();

        'pump: loop {
            let result = run_stdout_pump(
                ctx.clone(),
                thread_key.clone(),
                &sandbox_id,
                stdout,
                guard,
                &pipe,
            )
            .await;
            pipe.stdout_alive.store(false, Ordering::Release);
            let (execution, lines_pumped) = match result {
                Ok(StdoutPumpEnd::Idle) => break,
                Ok(StdoutPumpEnd::EofActiveExecution {
                    execution,
                    lines_pumped,
                }) => (execution, lines_pumped),
                Err(error) => {
                    warn!(
                        component = COMPONENT_SESSION_RUNTIME,
                        event = "session_stdout_pump_failed",
                        thread_key = %thread_key,
                        sandbox_id = %sandbox_id,
                        %error,
                        "session stdout pump failed"
                    );
                    let _ = ctx
                        .store
                        .append_event(
                            &thread_key,
                            None,
                            "session.stdout_pump_failed",
                            json!({
                                "sandbox_id": sandbox_id.as_str(),
                                "error": error.to_string(),
                            }),
                        )
                        .await;
                    break;
                }
            };

            if recover_detached_terminal_output(&ctx, &thread_key, &sandbox_id, &execution, &pipe)
                .await
                .unwrap_or_else(|error| {
                    warn!(
                        component = COMPONENT_SESSION_RUNTIME,
                        event = "session_stdout_recovery_failed",
                        thread_key = %thread_key,
                        sandbox_id = %sandbox_id,
                        execution_id = %execution.execution_id,
                        %error,
                        "failed to recover detached stdout from recorded output"
                    );
                    false
                })
            {
                break;
            }

            if lines_pumped > 0 {
                reattach_attempts = 0;
            }

            loop {
                if reattach_attempts >= SESSION_PIPE_MAX_REATTACH_ATTEMPTS {
                    fail_detached_execution(
                        &ctx,
                        &thread_key,
                        &sandbox_id,
                        &execution.execution_id,
                        &last_reattach_detail,
                    )
                    .await;
                    break 'pump;
                }
                reattach_attempts += 1;
                if reattach_attempts > 1 {
                    sleep(SESSION_PIPE_REATTACH_DELAY).await;
                }

                match reattach_session_pipe(&ctx, &open_lock, &sandbox_id, &pipe).await {
                    ReattachOutcome::Reattached {
                        pipe: new_pipe,
                        stdout: new_stdout,
                        guard: new_guard,
                    } => {
                        info!(
                            component = COMPONENT_SESSION_RUNTIME,
                            event = "session_stdout_pump_reattached",
                            thread_key = %thread_key,
                            sandbox_id = %sandbox_id,
                            execution_id = %execution.execution_id,
                            attempt = reattach_attempts,
                            "reattached session stdout pump after eof"
                        );
                        let _ = ctx
                            .store
                            .append_event(
                                &thread_key,
                                Some(&execution.execution_id),
                                "session.stdout_pump_reattached",
                                json!({
                                    "sandbox_id": sandbox_id.as_str(),
                                    "attempt": reattach_attempts,
                                }),
                            )
                            .await;
                        pipe = new_pipe;
                        stdout = new_stdout;
                        guard = new_guard;
                        continue 'pump;
                    }
                    ReattachOutcome::Superseded => return,
                    ReattachOutcome::Retryable(detail) => {
                        warn!(
                            component = COMPONENT_SESSION_RUNTIME,
                            event = "session_stdout_pump_reattach_failed",
                            thread_key = %thread_key,
                            sandbox_id = %sandbox_id,
                            execution_id = %execution.execution_id,
                            attempt = reattach_attempts,
                            detail = %detail,
                            "session stdout pump reattach attempt failed"
                        );
                        last_reattach_detail = detail;
                    }
                    ReattachOutcome::Dead(detail) => {
                        fail_detached_execution(
                            &ctx,
                            &thread_key,
                            &sandbox_id,
                            &execution.execution_id,
                            &detail,
                        )
                        .await;
                        break 'pump;
                    }
                }
            }
        }

        let _open_guard = open_lock.lock().await;
        remove_pipe_if_current(&ctx.sandbox_pipes, &sandbox_id, &pipe);
    });
}

async fn reattach_session_pipe(
    ctx: &RuntimeContext,
    open_lock: &SessionPipeOpenLock,
    sandbox_id: &str,
    pipe: &SessionPipe,
) -> ReattachOutcome {
    let _open_guard = open_lock.lock().await;
    if ctx
        .sandbox_pipes
        .get(sandbox_id)
        .is_none_or(|current| !Arc::ptr_eq(&current.stdin, &pipe.stdin))
    {
        return ReattachOutcome::Superseded;
    }

    let id = SandboxId::new(sandbox_id);
    match ctx.manager.status(&id).await {
        Ok(status) if status.can_open_io() => match ctx.manager.open_io(&id).await {
            Ok(io) => {
                let parts = io.into_parts();
                if pipe.resource_uid.as_deref() != parts.resource_uid.as_deref() {
                    return ReattachOutcome::Dead(
                        "sandbox io attachment no longer matches durable assignment".to_owned(),
                    );
                }
                let mut new_pipe = session_pipe_from_stdin(
                    parts.stdin,
                    pipe.output_state.clone(),
                    pipe.output_gate.clone(),
                );
                new_pipe.assignment_epoch = pipe.assignment_epoch.clone();
                new_pipe.resource_uid = pipe.resource_uid.clone();
                new_pipe.trace_assignment_epoch = pipe.trace_assignment_epoch.clone();
                new_pipe.trace_resource_uid = pipe.trace_resource_uid.clone();
                ctx.sandbox_pipes
                    .insert(sandbox_id.to_owned(), new_pipe.clone());
                spawn_stderr_drain(sandbox_id.to_owned(), parts.stderr);
                ReattachOutcome::Reattached {
                    pipe: new_pipe,
                    stdout: parts.stdout,
                    guard: parts.guard,
                }
            }
            Err(error) => {
                ReattachOutcome::Retryable(format!("sandbox stdout reattach failed: {error}"))
            }
        },
        Ok(status) => {
            ReattachOutcome::Dead(format!("sandbox no longer accepts io (status {status:?})"))
        }
        Err(SandboxError::NotFound(_)) => {
            ReattachOutcome::Dead("sandbox no longer exists".to_owned())
        }
        Err(error) => ReattachOutcome::Retryable(format!("sandbox status check failed: {error}")),
    }
}

async fn recover_detached_terminal_output(
    ctx: &RuntimeContext,
    thread_key: &ThreadKey,
    sandbox_id: &str,
    execution: &SessionExecution,
    pipe: &SessionPipe,
) -> Result<bool, SessionRuntimeError> {
    let since = execution.started_at.unwrap_or(execution.created_at);
    let id = SandboxId::new(sandbox_id);
    let lines = match ctx
        .manager
        .read_output_since(&id, Some(SystemTime::from(since)))
        .await
    {
        Ok(lines) => lines,
        Err(SandboxError::Unsupported { .. }) => return Ok(false),
        Err(error) => {
            warn!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "session_stdout_recorded_output_read_failed",
                thread_key = %thread_key,
                execution_id = %execution.execution_id,
                sandbox_id,
                %error,
                "failed to read recorded sandbox output; reattaching live"
            );
            return Ok(false);
        }
    };

    let _output_guard = pipe.output_read_guard().await;
    let live_output_state = pipe.output_state.lock().await.clone();
    let terminal =
        replay_detached_recorded_output(&live_output_state, &execution.execution_id, &lines);
    let Some(terminal) = terminal else {
        return Ok(false);
    };

    if !record_terminal_output(
        ctx,
        thread_key,
        sandbox_id,
        &execution.execution_id,
        terminal,
    )
    .await?
    {
        warn!(
            component = COMPONENT_SESSION_RUNTIME,
            event = "session_stdout_owner_lost",
            thread_key = %thread_key,
            execution_id = %execution.execution_id,
            sandbox_id,
            stdout_owner_id = %ctx.stdout_owner_id,
            "detached stdout recovery lost ownership before terminal persistence"
        );
        return Ok(true);
    }
    info!(
        component = COMPONENT_SESSION_RUNTIME,
        event = "session_stdout_pump_recovered",
        thread_key = %thread_key,
        execution_id = %execution.execution_id,
        sandbox_id,
        mode = "recorded_output",
        "recovered detached stdout pump from recorded sandbox output"
    );
    let _ = ctx
        .store
        .append_event(
            thread_key,
            Some(&execution.execution_id),
            "session.stdout_pump_recovered",
            json!({ "sandbox_id": sandbox_id, "mode": "recorded_output" }),
        )
        .await;
    Ok(true)
}

fn replay_detached_recorded_output(
    live_output_state: &StdoutPumpState,
    execution_id: &str,
    lines: &[String],
) -> Option<TerminalOutput> {
    let mut recorded_output_state = StdoutPumpState::default();
    if let Some(root_thread_id) = live_output_state.root_thread_id(execution_id) {
        // The live pump observed this execution's root before the detach. A
        // later recorded slice can start with a child thread.
        recorded_output_state.set_authoritative_root_thread_id(execution_id, root_thread_id);
    }
    let mut terminal = recorded_output_state.replay_recorded_output(execution_id, lines);
    if let Some(TerminalOutput::Completed { result_text, .. }) = terminal.as_mut()
        && let Some(live_final_answer) = live_output_state
            .final_answer_text_by_execution
            .get(execution_id)
            .filter(|text| !text.is_empty())
    {
        let recorded_is_canonical = recorded_output_state
            .canonical_final_answer_by_execution
            .contains(execution_id);
        let live_is_canonical = live_output_state
            .canonical_final_answer_by_execution
            .contains(execution_id);
        if !recorded_is_canonical {
            *result_text = if live_is_canonical {
                Some(live_final_answer.clone())
            } else if let Some(recorded_fragment) = result_text.as_deref() {
                Some(merge_answer_fragments(live_final_answer, recorded_fragment))
            } else {
                Some(live_final_answer.clone())
            };
        }
    }
    terminal
}

fn merge_answer_fragments(live: &str, recorded: &str) -> String {
    if recorded.starts_with(live) {
        return recorded.to_owned();
    }
    if live.starts_with(recorded) {
        return live.to_owned();
    }
    for (start, _) in live.char_indices() {
        let suffix = &live[start..];
        if let Some(remainder) = recorded.strip_prefix(suffix) {
            return format!("{live}{remainder}");
        }
    }
    format!("{live}{recorded}")
}

async fn fail_detached_execution(
    ctx: &RuntimeContext,
    thread_key: &ThreadKey,
    sandbox_id: &str,
    execution_id: &str,
    detail: &str,
) {
    let error = format!("sandbox stdout closed before terminal output; {detail}");
    if let Err(record_error) = record_terminal_output(
        ctx,
        thread_key,
        sandbox_id,
        execution_id,
        TerminalOutput::Failed { error },
    )
    .await
    {
        warn!(
            component = COMPONENT_SESSION_RUNTIME,
            event = "session_stdout_detached_fail_record_failed",
            thread_key = %thread_key,
            execution_id,
            sandbox_id,
            error = %record_error,
            "failed to record detached stdout failure"
        );
    }
}

async fn run_stdout_pump(
    ctx: RuntimeContext,
    thread_key: ThreadKey,
    sandbox_id: &str,
    stdout: SandboxRead,
    _guard: SandboxIoGuard,
    pipe: &SessionPipe,
) -> Result<StdoutPumpEnd, SessionRuntimeError> {
    let output_state = &pipe.output_state;
    let span = info_span!(
        "centaur.api_rs.session.stdout_pump",
        component = COMPONENT_SESSION_RUNTIME,
        event = "session_stdout_pump",
        "centaur.thread_key" = thread_key.as_str(),
        "centaur.sandbox_id" = sandbox_id,
        thread_key = %thread_key,
        sandbox_id,
    );
    set_span_parent_trace(
        &span,
        &thread_trace_id(&thread_key),
        &thread_trace_parent_span_id(&thread_key),
    );
    async {
        ensure_thread_trace_root_span(&thread_key);
        let mut stdout = FramedRead::new(stdout, LinesCodec::new());
        info!(
            component = COMPONENT_SESSION_RUNTIME,
            event = "session_stdout_pump_started",
            thread_key = %thread_key,
            sandbox_id,
            "session stdout pump started"
        );
        let mut reported_lost_stdout_ownership = HashSet::new();
        let mut line_count = 0_u64;
        while let Some(line) = stdout.next().await {
            let line = match line {
                Ok(line) => line,
                Err(error) => {
                    pipe.stdout_alive.store(false, Ordering::Release);
                    let message = stdout_pump_error_message(&error);
                    record_stdout_pump_failure(&ctx, &thread_key, sandbox_id, message).await?;
                    return Ok(StdoutPumpEnd::Idle);
                }
            };
            let _output_guard = pipe.output_read_guard().await;
            line_count += 1;
            let output_value = serde_json::from_str::<Value>(&line).ok();
            let active_execution = ctx.store.active_execution_for_thread(&thread_key).await?;
            let execution_id = active_execution
                .as_ref()
                .map(|execution| execution.execution_id.as_str());
            let (output_execution_id, should_record_first_token, harness_thread_id) = {
                let mut output_state = output_state.lock().await;
                let Some(output_execution_id) =
                    output_state.execution_for_line(execution_id, &line)
                else {
                    continue;
                };
                let should_record_first_token = output_state
                    .should_record_first_token(&output_execution_id, output_value.as_ref());
                let harness_thread_id =
                    harness_thread_id_from_output_line(&line).filter(|harness_thread_id| {
                        output_state.root_thread_id(&output_execution_id)
                            == Some(harness_thread_id.as_str())
                    });
                (
                    output_execution_id,
                    should_record_first_token,
                    harness_thread_id,
                )
            };
            let first_token_execution = active_execution
                .as_ref()
                .filter(|execution| {
                    execution.execution_id == output_execution_id && should_record_first_token
                })
                .cloned();
            let execution_span = ctx
                .execution_spans
                .lock()
                .await
                .get(&output_execution_id)
                .cloned();
            let output_span = output_state.lock().await.stdout_span_for_execution(
                execution_span.as_ref(),
                &thread_key,
                sandbox_id,
                &output_execution_id,
            );
            let Some(output_event) = append_output_line(
                &ctx,
                &thread_key,
                &output_execution_id,
                &line,
            )
            .instrument(output_span.clone())
            .await?
            else {
                if reported_lost_stdout_ownership.insert(output_execution_id.clone()) {
                    warn!(
                        component = COMPONENT_SESSION_RUNTIME,
                        event = "session_stdout_owner_lost",
                        thread_key = %thread_key,
                        execution_id = %output_execution_id,
                        sandbox_id,
                        stdout_owner_id = %ctx.stdout_owner_id,
                        "stdout pump does not own execution output; skipping row until ownership changes"
                    );
                }
                output_state.lock().await.forget(&output_execution_id);
                continue;
            };
            if reported_lost_stdout_ownership.remove(&output_execution_id) {
                info!(
                    component = COMPONENT_SESSION_RUNTIME,
                    event = "session_stdout_owner_recovered",
                    thread_key = %thread_key,
                    execution_id = %output_execution_id,
                    sandbox_id,
                    stdout_owner_id = %ctx.stdout_owner_id,
                    "stdout pump resumed execution output after ownership changed"
                );
            }
            if active_execution
                .as_ref()
                .is_some_and(|execution| execution.execution_id == output_execution_id)
                && let Some(harness_thread_id) = harness_thread_id
            {
                match ctx
                    .store
                    .update_harness_thread_id_if_stdout_owner(
                        &thread_key,
                        &output_execution_id,
                        &ctx.stdout_owner_id,
                        Some(&harness_thread_id),
                    )
                    .await
                {
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        warn!(
                            component = COMPONENT_SESSION_RUNTIME,
                            event = "session_stdout_owner_lost",
                            thread_key = %thread_key,
                            execution_id = %output_execution_id,
                            sandbox_id,
                            stdout_owner_id = %ctx.stdout_owner_id,
                            "stdout pump lost ownership before harness thread persistence"
                        );
                        output_state.lock().await.forget(&output_execution_id);
                        continue;
                    }
                    Err(error) => {
                        warn!(
                            %thread_key,
                            %harness_thread_id,
                            %error,
                            "failed to persist harness thread id"
                        );
                    }
                }
            }
            if let Some(execution) = first_token_execution {
                record_first_token_observation(
                    &ctx,
                    &thread_key,
                    &execution,
                    &output_event,
                    output_state,
                )
                .await;
            }
            if let Some(value) = output_value.as_ref() {
                output_state.lock().await.record_codex_app_server_spans(
                    &output_span,
                    &thread_key,
                    sandbox_id,
                    &output_execution_id,
                    value,
                );
            }
            let terminal = if active_execution
                .as_ref()
                .is_some_and(|execution| execution.execution_id == output_execution_id)
            {
                output_state
                    .lock()
                    .await
                    .observe(&output_execution_id, &line)
            } else {
                None
            };
            if let Some(terminal) = terminal {
                let terminalized = record_terminal_output(
                    &ctx,
                    &thread_key,
                    sandbox_id,
                    &output_execution_id,
                    terminal,
                )
                .instrument(output_span)
                .await?;
                if terminalized {
                    ctx.execution_spans.lock().await.remove(&output_execution_id);
                    output_state.lock().await.forget(&output_execution_id);
                } else {
                    warn!(
                        component = COMPONENT_SESSION_RUNTIME,
                        event = "session_stdout_owner_lost",
                        thread_key = %thread_key,
                        execution_id = %output_execution_id,
                        sandbox_id,
                        stdout_owner_id = %ctx.stdout_owner_id,
                        "stdout pump lost ownership before terminal persistence; retaining state for adoption"
                    );
                }
            }
        }
        pipe.stdout_alive.store(false, Ordering::Release);
        let active_execution = ctx.store.active_execution_for_thread(&thread_key).await?;
        ctx.store
            .append_event(
                &thread_key,
                None,
                "session.stdout_eof",
                json!({
                    "sandbox_id": sandbox_id,
                }),
            )
            .await?;
        info!(
            component = COMPONENT_SESSION_RUNTIME,
            event = "session_stdout_pump_completed",
            thread_key = %thread_key,
            sandbox_id,
            output_line_count = line_count,
            "session stdout pump completed"
        );
        match active_execution {
            Some(execution) => Ok(StdoutPumpEnd::EofActiveExecution {
                execution: Box::new(execution),
                lines_pumped: line_count,
            }),
            None => Ok(StdoutPumpEnd::Idle),
        }
    }
    .instrument(span)
    .await
}

async fn record_stdout_pump_failure(
    ctx: &RuntimeContext,
    thread_key: &ThreadKey,
    sandbox_id: &str,
    error: String,
) -> Result<(), SessionRuntimeError> {
    let active_execution = ctx.store.active_execution_for_thread(thread_key).await?;
    let execution_id = active_execution
        .as_ref()
        .map(|execution| execution.execution_id.clone());
    let terminalized_execution = if let Some(execution) = active_execution {
        record_terminal_output(
            ctx,
            thread_key,
            sandbox_id,
            &execution.execution_id,
            TerminalOutput::Failed {
                error: error.clone(),
            },
        )
        .await?
    } else {
        false
    };
    ctx.store
        .append_event(
            thread_key,
            execution_id.as_deref(),
            "session.stdout_pump_failed",
            json!({
                "sandbox_id": sandbox_id,
                "error": error.as_str(),
                "terminalized_execution": terminalized_execution,
            }),
        )
        .await?;
    Ok(())
}

async fn record_first_token_observation(
    ctx: &RuntimeContext,
    thread_key: &ThreadKey,
    execution: &SessionExecution,
    output_event: &SessionEvent,
    output_state: &SharedStdoutPumpState,
) {
    match ctx
        .store
        .execution_event_exists(&execution.execution_id, SESSION_FIRST_TOKEN_EVENT)
        .await
    {
        Ok(true) => {
            output_state
                .lock()
                .await
                .mark_first_token_recorded(&execution.execution_id);
            return;
        }
        Ok(false) => {}
        Err(error) => {
            warn!(
                component = COMPONENT_SESSION_RUNTIME,
                event = "session_first_token_marker_check_failed",
                thread_key = %thread_key,
                execution_id = %execution.execution_id,
                %error,
                "failed to check existing first-token marker"
            );
        }
    }

    let Some(latency) = first_token_latency(execution, output_event) else {
        output_state
            .lock()
            .await
            .mark_first_token_recorded(&execution.execution_id);
        return;
    };
    let harness_label = match ctx.store.get_session(thread_key).await {
        Ok(session) => session.harness_type.to_string(),
        Err(error) => {
            warn!(%thread_key, %error, "failed to load session for first-token metric labels");
            "unknown".to_owned()
        }
    };
    let latency_ms = duration_millis_u64(latency);
    if let Err(error) = ctx
        .store
        .append_event(
            thread_key,
            Some(&execution.execution_id),
            SESSION_FIRST_TOKEN_EVENT,
            json!({
                "execution_id": execution.execution_id.as_str(),
                "thread_key": thread_key.as_str(),
                "harness_type": harness_label.as_str(),
                "latency_ms": latency_ms,
                "output_event_id": output_event.event_id,
            }),
        )
        .await
    {
        warn!(
            component = COMPONENT_SESSION_RUNTIME,
            event = "session_first_token_marker_append_failed",
            thread_key = %thread_key,
            execution_id = %execution.execution_id,
            output_event_id = output_event.event_id,
            %error,
            "failed to append first-token marker"
        );
    }
    record_session_first_token_latency(&harness_label, latency);
    output_state
        .lock()
        .await
        .mark_first_token_recorded(&execution.execution_id);
    info!(
        component = COMPONENT_SESSION_RUNTIME,
        event = "session_first_token_observed",
        thread_key = %thread_key,
        execution_id = %execution.execution_id,
        harness_type = %harness_label,
        latency_ms,
        output_event_id = output_event.event_id,
        "session first answer token observed"
    );
}

fn first_token_latency(
    execution: &SessionExecution,
    output_event: &SessionEvent,
) -> Option<Duration> {
    let started_at = execution.started_at.unwrap_or(execution.created_at);
    (output_event.created_at - started_at).try_into().ok()
}

#[derive(Clone, Default)]
struct StdoutPumpState {
    final_answer_text_by_execution: HashMap<String, String>,
    canonical_final_answer_by_execution: HashSet<String>,
    first_token_recorded_by_execution: HashSet<String>,
    root_thread_id_by_execution: HashMap<String, String>,
    authoritative_root_by_execution: HashSet<String>,
    request_execution_by_id: HashMap<String, String>,
    completed_request_execution_by_id: HashMap<String, String>,
    completed_request_order: VecDeque<(String, String)>,
    turn_execution_by_id: HashMap<String, String>,
    completed_turn_execution_by_id: HashMap<String, String>,
    completed_turn_order: VecDeque<(String, String)>,
    item_execution_by_id: HashMap<String, String>,
    completed_item_execution_by_id: HashMap<String, String>,
    completed_item_order: VecDeque<(String, String)>,
    tool_call_by_id: HashMap<String, ToolCallLabels>,
    stdout_span_by_execution: HashMap<String, Span>,
}

impl StdoutPumpState {
    fn merge_from(&mut self, incoming: Self) {
        for (execution_id, text) in incoming.final_answer_text_by_execution {
            if text.is_empty()
                || self
                    .canonical_final_answer_by_execution
                    .contains(&execution_id)
            {
                continue;
            }
            if incoming
                .canonical_final_answer_by_execution
                .contains(&execution_id)
            {
                self.final_answer_text_by_execution
                    .insert(execution_id.clone(), text);
                self.canonical_final_answer_by_execution
                    .insert(execution_id);
                continue;
            }
            let current = self
                .final_answer_text_by_execution
                .entry(execution_id)
                .or_default();
            if current.is_empty()
                || text.starts_with(current.as_str())
                || text.len() > current.len()
            {
                *current = text;
            }
        }
        self.first_token_recorded_by_execution
            .extend(incoming.first_token_recorded_by_execution);
        for (execution_id, root_thread_id) in incoming.root_thread_id_by_execution {
            if incoming
                .authoritative_root_by_execution
                .contains(&execution_id)
            {
                self.root_thread_id_by_execution
                    .insert(execution_id.clone(), root_thread_id);
                self.authoritative_root_by_execution.insert(execution_id);
            } else if !self.authoritative_root_by_execution.contains(&execution_id) {
                self.root_thread_id_by_execution
                    .entry(execution_id)
                    .or_insert(root_thread_id);
            }
        }
        for (output_id, execution_id) in incoming.completed_request_order {
            if incoming.completed_request_execution_by_id.get(&output_id) != Some(&execution_id) {
                continue;
            }
            remember_completed_output_id(
                &mut self.completed_request_execution_by_id,
                &mut self.completed_request_order,
                output_id,
                execution_id,
            );
        }
        for (output_id, execution_id) in incoming.completed_turn_order {
            if incoming.completed_turn_execution_by_id.get(&output_id) != Some(&execution_id) {
                continue;
            }
            remember_completed_output_id(
                &mut self.completed_turn_execution_by_id,
                &mut self.completed_turn_order,
                output_id,
                execution_id,
            );
        }
        for (output_id, execution_id) in incoming.completed_item_order {
            if incoming.completed_item_execution_by_id.get(&output_id) != Some(&execution_id) {
                continue;
            }
            remember_completed_output_id(
                &mut self.completed_item_execution_by_id,
                &mut self.completed_item_order,
                output_id,
                execution_id,
            );
        }
        for (request_id, execution_id) in incoming.request_execution_by_id {
            self.completed_request_execution_by_id.remove(&request_id);
            self.completed_request_order
                .retain(|(known_id, _)| known_id != &request_id);
            self.request_execution_by_id
                .insert(request_id, execution_id);
        }
        for (turn_id, execution_id) in incoming.turn_execution_by_id {
            self.completed_turn_execution_by_id.remove(&turn_id);
            self.completed_turn_order
                .retain(|(known_id, _)| known_id != &turn_id);
            self.turn_execution_by_id.insert(turn_id, execution_id);
        }
        for (item_id, execution_id) in incoming.item_execution_by_id {
            self.completed_item_execution_by_id.remove(&item_id);
            self.completed_item_order
                .retain(|(known_id, _)| known_id != &item_id);
            self.item_execution_by_id.insert(item_id, execution_id);
        }
        self.tool_call_by_id.extend(incoming.tool_call_by_id);
        for (execution_id, span) in incoming.stdout_span_by_execution {
            self.stdout_span_by_execution
                .entry(execution_id)
                .or_insert(span);
        }
    }

    fn replay_recorded_output(
        &mut self,
        execution_id: &str,
        lines: &[String],
    ) -> Option<TerminalOutput> {
        // A durable Codex app-server root is authoritative on later turns,
        // whose log slice may contain only child thread starts. Nanocodex does
        // not seed its per-run request id, so its first recorded run start is
        // still discovered here.
        if !self.authoritative_root_by_execution.contains(execution_id)
            && let Some(root_thread_id) = lines.iter().find_map(|line| {
                serde_json::from_str::<Value>(line)
                    .ok()
                    .and_then(|value| root_thread_id_from_output(&value))
            })
        {
            self.root_thread_id_by_execution
                .insert(execution_id.to_owned(), root_thread_id);
        }
        self.replay_output_lines(execution_id, lines)
    }

    fn replay_output_lines(
        &mut self,
        execution_id: &str,
        lines: &[String],
    ) -> Option<TerminalOutput> {
        for line in lines {
            let Some(output_execution_id) = self.execution_for_line(Some(execution_id), line)
            else {
                continue;
            };
            if let Some(terminal) = self.observe(&output_execution_id, line) {
                return Some(terminal);
            }
        }
        None
    }

    fn execution_for_line(
        &mut self,
        active_execution_id: Option<&str>,
        line: &str,
    ) -> Option<String> {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            return active_execution_id.map(ToOwned::to_owned);
        };

        if let Some(known_execution_id) = self.known_execution_for_value(&value) {
            self.remember_root_thread_id(&value, &known_execution_id);
            if active_execution_id == Some(known_execution_id.as_str()) {
                self.remember_value_execution(&value, &known_execution_id);
                return Some(known_execution_id);
            }
            if terminal_output(
                &value,
                self.final_answer_text_by_execution
                    .get(&known_execution_id)
                    .map(String::as_str)
                    .unwrap_or(""),
            )
            .is_some()
            {
                self.forget(&known_execution_id);
            }
            return None;
        }

        let active_execution_id = active_execution_id?;
        self.remember_root_thread_id(&value, active_execution_id);
        self.remember_value_execution(&value, active_execution_id);
        Some(active_execution_id.to_owned())
    }

    fn observe(&mut self, execution_id: &str, line: &str) -> Option<TerminalOutput> {
        let value: Value = serde_json::from_str(line).ok()?;
        self.remember_root_thread_id(&value, execution_id);
        if !self.is_root_thread_event(execution_id, &value) {
            return None;
        }
        if let Some(update) = output_line_final_answer_text(&value) {
            match update {
                FinalAnswerTextUpdate::Append(delta) => self
                    .final_answer_text_by_execution
                    .entry(execution_id.to_owned())
                    .or_default()
                    .push_str(&delta),
                FinalAnswerTextUpdate::Replace(canonical) => {
                    self.final_answer_text_by_execution
                        .insert(execution_id.to_owned(), canonical);
                    self.canonical_final_answer_by_execution
                        .insert(execution_id.to_owned());
                }
            }
        }
        terminal_output(
            &value,
            self.final_answer_text_by_execution
                .get(execution_id)
                .map(String::as_str)
                .unwrap_or(""),
        )
    }

    fn should_record_first_token(&self, execution_id: &str, value: Option<&Value>) -> bool {
        if self
            .first_token_recorded_by_execution
            .contains(execution_id)
            || self
                .final_answer_text_by_execution
                .get(execution_id)
                .is_some_and(|text| !text.trim().is_empty())
        {
            return false;
        }

        let Some(value) = value else {
            return false;
        };
        if !self.is_root_thread_event(execution_id, value) {
            return false;
        }
        if output_line_final_answer_text(value).is_some() {
            return true;
        }
        matches!(
            terminal_output(value, ""),
            Some(TerminalOutput::Completed {
                result_text: Some(_),
                ..
            })
        )
    }

    fn mark_first_token_recorded(&mut self, execution_id: &str) {
        self.first_token_recorded_by_execution
            .insert(execution_id.to_owned());
    }

    fn forget(&mut self, execution_id: &str) {
        self.final_answer_text_by_execution.remove(execution_id);
        self.canonical_final_answer_by_execution
            .remove(execution_id);
        self.first_token_recorded_by_execution.remove(execution_id);
        self.root_thread_id_by_execution.remove(execution_id);
        self.authoritative_root_by_execution.remove(execution_id);
        let request_ids_to_forget = self
            .request_execution_by_id
            .iter()
            .filter(|&(_request_id, mapped_execution_id)| mapped_execution_id == execution_id)
            .map(|(request_id, _mapped_execution_id)| request_id.clone())
            .collect::<Vec<_>>();
        let turn_ids_to_forget = self
            .turn_execution_by_id
            .iter()
            .filter(|&(_turn_id, mapped_execution_id)| mapped_execution_id == execution_id)
            .map(|(turn_id, _mapped_execution_id)| turn_id.clone())
            .collect::<Vec<_>>();
        let tool_ids_to_forget = self
            .item_execution_by_id
            .iter()
            .filter(|&(_item_id, mapped_execution_id)| mapped_execution_id == execution_id)
            .map(|(item_id, _mapped_execution_id)| item_id.clone())
            .collect::<Vec<_>>();
        for request_id in request_ids_to_forget {
            self.request_execution_by_id.remove(&request_id);
            remember_completed_output_id(
                &mut self.completed_request_execution_by_id,
                &mut self.completed_request_order,
                request_id,
                execution_id.to_owned(),
            );
        }
        for turn_id in turn_ids_to_forget {
            self.turn_execution_by_id.remove(&turn_id);
            remember_completed_output_id(
                &mut self.completed_turn_execution_by_id,
                &mut self.completed_turn_order,
                turn_id,
                execution_id.to_owned(),
            );
        }
        for item_id in &tool_ids_to_forget {
            self.item_execution_by_id.remove(item_id);
            remember_completed_output_id(
                &mut self.completed_item_execution_by_id,
                &mut self.completed_item_order,
                item_id.clone(),
                execution_id.to_owned(),
            );
        }
        self.stdout_span_by_execution.remove(execution_id);
        for item_id in tool_ids_to_forget {
            self.tool_call_by_id.remove(&item_id);
        }
    }

    fn stdout_span_for_execution(
        &mut self,
        parent: Option<&Span>,
        thread_key: &ThreadKey,
        sandbox_id: &str,
        execution_id: &str,
    ) -> Span {
        if let Some(span) = self.stdout_span_by_execution.get(execution_id) {
            return span.clone();
        }
        let span = new_stdout_pump_span(parent, thread_key, sandbox_id, execution_id);
        self.stdout_span_by_execution
            .insert(execution_id.to_owned(), span.clone());
        span
    }

    fn record_codex_app_server_spans(
        &mut self,
        parent: &Span,
        thread_key: &ThreadKey,
        sandbox_id: &str,
        execution_id: &str,
        value: &Value,
    ) {
        record_codex_app_server_event_span(parent, thread_key, sandbox_id, execution_id, value);
        for event in tool_call_span_events(value, &mut self.tool_call_by_id) {
            record_codex_app_server_tool_span(parent, thread_key, sandbox_id, execution_id, &event);
        }
    }

    fn known_execution_for_value(&self, value: &Value) -> Option<String> {
        if let Some(request_id) = output_request_id(value)
            && let Some(execution_id) = self
                .request_execution_by_id
                .get(request_id)
                .or_else(|| self.completed_request_execution_by_id.get(request_id))
        {
            return Some(execution_id.clone());
        }
        for turn_id in turn_ids(value) {
            if let Some(execution_id) = self
                .turn_execution_by_id
                .get(&turn_id)
                .or_else(|| self.completed_turn_execution_by_id.get(&turn_id))
            {
                return Some(execution_id.clone());
            }
        }
        for item_id in item_ids(value) {
            if let Some(execution_id) = self
                .item_execution_by_id
                .get(&item_id)
                .or_else(|| self.completed_item_execution_by_id.get(&item_id))
            {
                return Some(execution_id.clone());
            }
        }
        None
    }

    fn remember_value_execution(&mut self, value: &Value, execution_id: &str) {
        if let Some(request_id) = output_request_id(value) {
            self.completed_request_execution_by_id.remove(request_id);
            self.completed_request_order
                .retain(|(known_id, _)| known_id != request_id);
            self.request_execution_by_id
                .insert(request_id.to_owned(), execution_id.to_owned());
        }
        for turn_id in turn_ids(value) {
            self.completed_turn_execution_by_id.remove(&turn_id);
            self.completed_turn_order
                .retain(|(known_id, _)| known_id != &turn_id);
            self.turn_execution_by_id
                .insert(turn_id, execution_id.to_owned());
        }
        for item_id in item_ids(value) {
            self.completed_item_execution_by_id.remove(&item_id);
            self.completed_item_order
                .retain(|(known_id, _)| known_id != &item_id);
            self.item_execution_by_id
                .insert(item_id, execution_id.to_owned());
        }
    }

    fn remember_root_thread_id(&mut self, value: &Value, execution_id: &str) {
        if let Some(thread_id) = root_thread_id_from_output(value) {
            self.seed_root_thread_id_if_absent(execution_id, &thread_id);
        }
    }

    fn seed_root_thread_id_if_absent(&mut self, execution_id: &str, thread_id: &str) {
        let thread_id = thread_id.trim();
        if !thread_id.is_empty() {
            self.root_thread_id_by_execution
                .entry(execution_id.to_owned())
                .or_insert_with(|| thread_id.to_owned());
        }
    }

    fn set_authoritative_root_thread_id(&mut self, execution_id: &str, thread_id: &str) {
        let thread_id = thread_id.trim();
        if !thread_id.is_empty() {
            self.root_thread_id_by_execution
                .insert(execution_id.to_owned(), thread_id.to_owned());
            self.authoritative_root_by_execution
                .insert(execution_id.to_owned());
        }
    }

    fn root_thread_id(&self, execution_id: &str) -> Option<&str> {
        self.root_thread_id_by_execution
            .get(execution_id)
            .map(String::as_str)
    }

    fn is_root_thread_event(&self, execution_id: &str, value: &Value) -> bool {
        self.root_thread_id_by_execution
            .get(execution_id)
            .is_none_or(|thread_id| output_belongs_to_thread(value, thread_id))
    }
}

fn remember_completed_output_id(
    completed: &mut HashMap<String, String>,
    order: &mut VecDeque<(String, String)>,
    output_id: String,
    execution_id: String,
) {
    order.retain(|(known_id, _)| known_id != &output_id);
    completed.insert(output_id.clone(), execution_id.clone());
    order.push_back((output_id, execution_id));
    while order.len() > COMPLETED_OUTPUT_ID_CAPACITY {
        let Some((expired_id, expired_execution_id)) = order.pop_front() else {
            break;
        };
        if completed.get(&expired_id) == Some(&expired_execution_id) {
            completed.remove(&expired_id);
        }
    }
}

fn new_stdout_pump_span(
    parent: Option<&Span>,
    thread_key: &ThreadKey,
    sandbox_id: &str,
    execution_id: &str,
) -> Span {
    if let Some(parent) = parent {
        info_span!(
            parent: parent,
            "centaur.api_rs.session.stdout_pump",
            component = COMPONENT_SESSION_RUNTIME,
            event = "session_stdout_pump",
            "centaur.thread_key" = thread_key.as_str(),
            "centaur.execution_id" = execution_id,
            "centaur.sandbox_id" = sandbox_id,
            thread_key = %thread_key,
            execution_id,
            sandbox_id,
        )
    } else {
        info_span!(
            "centaur.api_rs.session.stdout_pump",
            component = COMPONENT_SESSION_RUNTIME,
            event = "session_stdout_pump",
            "centaur.thread_key" = thread_key.as_str(),
            "centaur.execution_id" = execution_id,
            "centaur.sandbox_id" = sandbox_id,
            thread_key = %thread_key,
            execution_id,
            sandbox_id,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ToolCallLabels {
    kind: String,
    name: String,
    method: String,
}

#[derive(Clone, Debug, PartialEq)]
struct ToolCallSpanEvent {
    labels: ToolCallLabels,
    status: &'static str,
    duration: Option<Duration>,
}

fn record_codex_app_server_event_span(
    parent: &Span,
    thread_key: &ThreadKey,
    sandbox_id: &str,
    execution_id: &str,
    value: &Value,
) {
    let event_type = sandbox_output_event_type(value);
    let source = sandbox_output_source(value);
    let item = protocol_item(value);
    let item_type = item
        .and_then(|item| string_at_path(item, &["type"]))
        .unwrap_or_default();
    let turn_id = turn_ids(value).into_iter().next().unwrap_or_default();
    let item_id = item_ids(value).into_iter().next().unwrap_or_default();

    let span = info_span!(
        parent: parent,
        "centaur.api_rs.codex_app_server.event",
        component = COMPONENT_SESSION_RUNTIME,
        event = "codex_app_server_event",
        "centaur.thread_key" = thread_key.as_str(),
        "centaur.execution_id" = execution_id,
        "centaur.sandbox_id" = sandbox_id,
        "codex_app_server.source" = source,
        "codex_app_server.event_type" = event_type,
        "codex_app_server.item_type" = item_type.as_str(),
        "codex_app_server.turn_id" = turn_id.as_str(),
        "codex_app_server.item_id" = item_id.as_str(),
    );
    let _entered = span.enter();
}

fn record_codex_app_server_tool_span(
    parent: &Span,
    thread_key: &ThreadKey,
    sandbox_id: &str,
    execution_id: &str,
    event: &ToolCallSpanEvent,
) {
    let duration_ms = event
        .duration
        .map(|duration| duration.as_secs_f64() * 1000.0);
    let span = info_span!(
        parent: parent,
        "centaur.api_rs.codex_app_server.tool_call",
        component = COMPONENT_SESSION_RUNTIME,
        event = "codex_app_server_tool_call",
        "centaur.thread_key" = thread_key.as_str(),
        "centaur.execution_id" = execution_id,
        "centaur.sandbox_id" = sandbox_id,
        "tool.kind" = event.labels.kind.as_str(),
        "tool.name" = event.labels.name.as_str(),
        "tool.method" = event.labels.method.as_str(),
        "tool.status" = event.status,
        "tool.duration_ms" = tracing::field::Empty,
    );
    if let Some(duration_ms) = duration_ms {
        span.record("tool.duration_ms", duration_ms);
    }
    let _entered = span.enter();
}

fn sandbox_output_event_type(value: &Value) -> &str {
    value
        .get("method")
        .and_then(Value::as_str)
        .or_else(|| value.get("type").and_then(Value::as_str))
        .filter(|event_type| !event_type.trim().is_empty())
        .unwrap_or("json")
}

fn sandbox_output_source(value: &Value) -> &str {
    if value.get("method").and_then(Value::as_str).is_some() {
        return "codex_app_server";
    }
    match value.get("type").and_then(Value::as_str) {
        Some(event_type)
            if event_type.starts_with("item.")
                || event_type.starts_with("turn.")
                || event_type.starts_with("thread.") =>
        {
            "codex_app_server"
        }
        Some("system")
            if value
                .get("subtype")
                .and_then(Value::as_str)
                .is_some_and(|subtype| subtype.starts_with("wrapper_")) =>
        {
            "codex_app_server"
        }
        Some("assistant" | "user" | "tool") => "harness",
        Some(_) | None => "sandbox",
    }
}

fn tool_call_span_events(
    value: &Value,
    known_tool_calls: &mut HashMap<String, ToolCallLabels>,
) -> Vec<ToolCallSpanEvent> {
    let mut events = Vec::new();
    let event_type = sandbox_output_event_type(value);

    if matches!(event_type, "item/started" | "item.started")
        && let Some(item) = protocol_item(value)
        && let Some(labels) = tool_labels_from_item(item)
    {
        remember_tool_call_labels(item, &labels, known_tool_calls);
        events.push(ToolCallSpanEvent {
            labels,
            status: "started",
            duration: None,
        });
    }

    if matches!(event_type, "item/completed" | "item.completed")
        && let Some(item) = protocol_item(value)
    {
        let item_id = string_at_path(item, &["id"]);
        let labels = tool_labels_from_item(item).or_else(|| {
            item_id
                .as_deref()
                .and_then(|item_id| known_tool_calls.get(item_id).cloned())
        });
        if let Some(labels) = labels {
            let status = completed_tool_status(item);
            if let Some(item_id) = item_id {
                known_tool_calls.remove(&item_id);
            }
            events.push(ToolCallSpanEvent {
                labels,
                status,
                duration: duration_from_ms_value(
                    item.get("durationMs").or_else(|| item.get("duration_ms")),
                ),
            });
        }
    }

    if matches!(
        event_type,
        "item/mcpToolCall/progress" | "item.mcpToolCall.progress"
    ) {
        let labels = progress_item_id(value)
            .and_then(|item_id| known_tool_calls.get(&item_id).cloned())
            .unwrap_or_else(|| ToolCallLabels {
                kind: "mcp".to_owned(),
                name: "unknown".to_owned(),
                method: "unknown".to_owned(),
            });
        events.push(ToolCallSpanEvent {
            labels,
            status: "progress",
            duration: None,
        });
    }

    for tool_use in anthropic_tool_uses(value) {
        let labels = ToolCallLabels {
            kind: "anthropic".to_owned(),
            name: string_at_path(tool_use, &["name"]).unwrap_or_else(|| "unknown".to_owned()),
            method: "call".to_owned(),
        };
        if let Some(tool_id) = string_at_path(tool_use, &["id"]) {
            known_tool_calls.insert(tool_id, labels.clone());
        }
        events.push(ToolCallSpanEvent {
            labels,
            status: "started",
            duration: None,
        });
    }

    for tool_result in anthropic_tool_results(value) {
        let labels = string_at_path(tool_result, &["tool_use_id"])
            .and_then(|tool_use_id| known_tool_calls.remove(&tool_use_id))
            .unwrap_or_else(|| ToolCallLabels {
                kind: "anthropic".to_owned(),
                name: "unknown".to_owned(),
                method: "call".to_owned(),
            });
        events.push(ToolCallSpanEvent {
            labels,
            status: if tool_result
                .get("is_error")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                "failed"
            } else {
                "completed"
            },
            duration: None,
        });
    }

    events
}

fn protocol_item(value: &Value) -> Option<&Value> {
    value
        .get("params")
        .and_then(|params| params.get("item"))
        .or_else(|| value.get("item"))
}

fn tool_labels_from_item(item: &Value) -> Option<ToolCallLabels> {
    let item_type = string_at_path(item, &["type"])?;
    match item_type.as_str() {
        "mcpToolCall" | "mcp_tool_call" => Some(ToolCallLabels {
            kind: "mcp".to_owned(),
            name: string_at_path(item, &["tool"]).unwrap_or_else(|| "unknown".to_owned()),
            method: string_at_path(item, &["server"]).unwrap_or_else(|| "call".to_owned()),
        }),
        "dynamicToolCall" | "dynamic_tool_call" => Some(ToolCallLabels {
            kind: "dynamic".to_owned(),
            name: string_at_path(item, &["tool"]).unwrap_or_else(|| "unknown".to_owned()),
            method: string_at_path(item, &["namespace"]).unwrap_or_else(|| "call".to_owned()),
        }),
        "collabAgentToolCall" | "collab_agent_tool_call" => Some(ToolCallLabels {
            kind: "collab_agent".to_owned(),
            name: string_at_path(item, &["tool"]).unwrap_or_else(|| "agent".to_owned()),
            method: "call".to_owned(),
        }),
        _ => None,
    }
}

fn remember_tool_call_labels(
    item: &Value,
    labels: &ToolCallLabels,
    known_tool_calls: &mut HashMap<String, ToolCallLabels>,
) {
    if let Some(item_id) = string_at_path(item, &["id"]) {
        known_tool_calls.insert(item_id, labels.clone());
    }
}

fn completed_tool_status(item: &Value) -> &'static str {
    if item
        .get("success")
        .and_then(Value::as_bool)
        .is_some_and(|success| !success)
        || item.get("error").is_some()
    {
        return "failed";
    }

    if let Some(exit_code) = item.get("exitCode").and_then(Value::as_i64) {
        return if exit_code == 0 {
            "completed"
        } else {
            "failed"
        };
    }

    match item
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("completed")
    {
        "failed" | "error" | "cancelled" | "declined" => "failed",
        "inProgress" | "in_progress" | "running" => "started",
        _ => "completed",
    }
}

fn duration_from_ms_value(value: Option<&Value>) -> Option<Duration> {
    let millis = value.and_then(|value| {
        value
            .as_f64()
            .or_else(|| value.as_u64().map(|millis| millis as f64))
            .or_else(|| value.as_i64().map(|millis| millis as f64))
    })?;
    if millis.is_finite() && millis >= 0.0 {
        Some(Duration::from_secs_f64(millis / 1000.0))
    } else {
        None
    }
}

fn progress_item_id(value: &Value) -> Option<String> {
    [
        &["params", "itemId"][..],
        &["params", "item_id"][..],
        &["itemId"][..],
        &["item_id"][..],
    ]
    .into_iter()
    .filter_map(|path| string_at_path(value, path))
    .next()
}

fn anthropic_tool_uses(value: &Value) -> Vec<&Value> {
    if value.get("type").and_then(Value::as_str) != Some("assistant") {
        return Vec::new();
    }
    content_blocks(value)
        .into_iter()
        .filter(|part| part.get("type").and_then(Value::as_str) == Some("tool_use"))
        .collect()
}

fn anthropic_tool_results(value: &Value) -> Vec<&Value> {
    if !matches!(
        value.get("type").and_then(Value::as_str),
        Some("user" | "tool")
    ) {
        return Vec::new();
    }
    content_blocks(value)
        .into_iter()
        .filter(|part| {
            part.get("type").and_then(Value::as_str) == Some("tool_result")
                || part.get("tool_use_id").and_then(Value::as_str).is_some()
        })
        .collect()
}

fn content_blocks(value: &Value) -> Vec<&Value> {
    value
        .get("content")
        .or_else(|| {
            value
                .get("message")
                .and_then(|message| message.get("content"))
        })
        .and_then(Value::as_array)
        .map(|values| values.iter().collect())
        .unwrap_or_default()
}

#[derive(Debug, Eq, PartialEq)]
enum TerminalOutput {
    Completed {
        reason: &'static str,
        result_text: Option<String>,
    },
    Cancelled {
        reason: &'static str,
    },
    Failed {
        error: String,
    },
}

async fn record_terminal_output(
    ctx: &RuntimeContext,
    thread_key: &ThreadKey,
    sandbox_id: &str,
    execution_id: &str,
    terminal: TerminalOutput,
) -> Result<bool, SessionRuntimeError> {
    let mut failure_class = None;
    let (owned_terminal, terminal_status) = match terminal {
        TerminalOutput::Completed {
            reason,
            result_text,
        } => {
            let mut payload = json!({
                "execution_id": execution_id,
                "thread_key": thread_key.as_str(),
                "completion_reason": reason,
            });
            if let (Some(result_text), Some(object)) =
                (result_text.as_deref(), payload.as_object_mut())
            {
                object.insert("result_text".to_owned(), json!(result_text));
            }
            (OwnedTerminalEvent::Completed { payload }, "completed")
        }
        TerminalOutput::Cancelled { reason } => (
            OwnedTerminalEvent::Cancelled {
                reason: reason.to_owned(),
                payload: json!({
                    "execution_id": execution_id,
                    "thread_key": thread_key.as_str(),
                    "reason": reason,
                }),
            },
            "cancelled",
        ),
        TerminalOutput::Failed { error } => {
            failure_class = Some(terminal_failure_class(&error));
            (
                OwnedTerminalEvent::Failed {
                    payload: json!({
                        "execution_id": execution_id,
                        "thread_key": thread_key.as_str(),
                        "error": error.as_str(),
                    }),
                    error,
                },
                "failed",
            )
        }
    };
    let Some((terminal_execution, _)) = ctx
        .store
        .terminalize_execution_and_append_event_if_stdout_owner(
            execution_id,
            &ctx.stdout_owner_id,
            owned_terminal,
        )
        .await?
    else {
        return Ok(false);
    };
    stop_terminal_stdout_owner_renewer(ctx, execution_id).await;
    ctx.execution_spans.lock().await.remove(execution_id);
    if let Err(error) = ctx
        .store
        .touch_sandbox_activity(thread_key, sandbox_id)
        .await
    {
        warn!(
            component = COMPONENT_SESSION_RUNTIME,
            event = "session_sandbox_activity_touch_failed",
            thread_key = %thread_key,
            execution_id,
            sandbox_id,
            %error,
            "failed to touch sandbox activity after terminal output"
        );
    }
    record_finished_execution_metric(
        &ctx.store,
        thread_key,
        &terminal_execution,
        terminal_status,
        failure_class,
    )
    .await;
    if let Some(idle_timeout) = idle_timeout_from_execution(&terminal_execution) {
        spawn_idle_pause(
            ctx.clone(),
            thread_key.clone(),
            terminal_execution.execution_id,
            sandbox_id.to_owned(),
            idle_timeout,
        );
    }
    Ok(true)
}

fn spawn_max_duration_failure(
    ctx: RuntimeContext,
    thread_key: ThreadKey,
    execution_id: String,
    max_duration: Duration,
    idle_timeout: Option<Duration>,
) {
    spawn_max_duration_failure_after(
        ctx,
        thread_key,
        execution_id,
        max_duration,
        max_duration,
        idle_timeout,
    );
}

/// Fails after `sleep_duration` but reports the durable configured limit.
/// Recovery only has the remaining sleep budget, whereas durable events must
/// continue to identify the original caller-supplied `max_duration_ms`.
fn spawn_max_duration_failure_after(
    ctx: RuntimeContext,
    thread_key: ThreadKey,
    execution_id: String,
    sleep_duration: Duration,
    max_duration: Duration,
    idle_timeout: Option<Duration>,
) {
    tokio::spawn(async move {
        sleep(sleep_duration).await;
        if let Err(error) = record_max_duration_failure(
            &ctx,
            &thread_key,
            &execution_id,
            max_duration,
            idle_timeout,
        )
        .await
        {
            warn!(%thread_key, %execution_id, %error, "max duration failure task failed");
        }
    });
}

/// Re-arm a persisted deadline after a process recovers execution ownership.
/// `started_at` is durable, so recovery must consume the elapsed budget rather
/// than grant a fresh full-duration turn.
fn spawn_remaining_max_duration_failure(ctx: RuntimeContext, execution: &SessionExecution) {
    let Some(max_duration) = max_duration_from_execution(execution) else {
        return;
    };
    let since = execution.started_at.unwrap_or(execution.created_at);
    let elapsed = SystemTime::now()
        .duration_since(SystemTime::from(since))
        .unwrap_or_default();
    spawn_max_duration_failure_after(
        ctx,
        execution.thread_key.clone(),
        execution.execution_id.clone(),
        max_duration.saturating_sub(elapsed),
        max_duration,
        idle_timeout_from_execution(execution),
    );
}

fn spawn_stdout_owner_renewer(ctx: RuntimeContext, execution_id: String) {
    let (cancel, mut cancelled) = tokio::sync::watch::channel(false);
    let (stopped, _) = tokio::sync::watch::channel(false);
    let renewal = Arc::new(StdoutOwnerRenewal {
        generation: Uuid::new_v4(),
        cancel,
        stopped,
        #[cfg(test)]
        renew_now: tokio::sync::Notify::new(),
        #[cfg(test)]
        renew_db_started: tokio::sync::Notify::new(),
    });
    ctx.stdout_owner_renewals
        .insert(execution_id.clone(), renewal.clone());
    let registry = ctx.stdout_owner_renewals.clone();
    tokio::spawn(async move {
        loop {
            #[cfg(not(test))]
            {
                tokio::select! {
                    biased;
                    changed = cancelled.changed() => {
                        if changed.is_err() || *cancelled.borrow() {
                            break;
                        }
                    }
                    _ = sleep(STDOUT_OWNER_RENEW_INTERVAL) => {}
                }
            }
            #[cfg(test)]
            {
                tokio::select! {
                    biased;
                    changed = cancelled.changed() => {
                        if changed.is_err() || *cancelled.borrow() {
                            break;
                        }
                    }
                    _ = renewal.renew_now.notified() => {}
                    _ = sleep(STDOUT_OWNER_RENEW_INTERVAL) => {}
                }
                renewal.renew_db_started.notify_one();
            }
            let renewed = ctx
                .store
                .renew_stdout_owner(&execution_id, &ctx.stdout_owner_id, STDOUT_OWNER_LEASE)
                .await;
            match renewed {
                Ok(true) => {}
                Ok(false) => break,
                Err(error) => {
                    warn!(
                        component = COMPONENT_SESSION_RUNTIME,
                        event = "session_stdout_owner_renew_failed",
                        execution_id,
                        stdout_owner_id = %ctx.stdout_owner_id,
                        %error,
                        "failed to renew stdout owner lease"
                    );
                    break;
                }
            }
        }
        renewal.stopped.send_replace(true);
        registry.remove_if(&execution_id, |_, current| {
            current.generation == renewal.generation
        });
    });
}

async fn stop_stdout_owner_renewer(
    registry: &StdoutOwnerRenewalRegistry,
    execution_id: &str,
) -> bool {
    let renewal = registry
        .get(execution_id)
        .map(|entry| Arc::clone(entry.value()));
    let Some(renewal) = renewal else {
        return true;
    };
    let _ = renewal.cancel.send(true);
    if timeout(STDOUT_OWNER_RENEWER_STOP_TIMEOUT, renewal.wait_stopped())
        .await
        .is_err()
    {
        return false;
    }
    registry.remove_if(execution_id, |_, current| {
        current.generation == renewal.generation
    });
    true
}

async fn stop_all_stdout_owner_renewers(registry: &StdoutOwnerRenewalRegistry) -> bool {
    let renewals = registry
        .iter()
        .map(|entry| (entry.key().clone(), Arc::clone(entry.value())))
        .collect::<Vec<_>>();
    for (_, renewal) in &renewals {
        let _ = renewal.cancel.send(true);
    }
    if timeout(STDOUT_OWNER_RENEWER_STOP_TIMEOUT, async {
        for (_, renewal) in &renewals {
            renewal.wait_stopped().await;
        }
    })
    .await
    .is_err()
    {
        return false;
    }
    for (execution_id, renewal) in renewals {
        registry.remove_if(&execution_id, |_, current| {
            current.generation == renewal.generation
        });
    }
    true
}

async fn stop_terminal_stdout_owner_renewer(ctx: &RuntimeContext, execution_id: &str) {
    if !stop_stdout_owner_renewer(&ctx.stdout_owner_renewals, execution_id).await {
        warn!(
            component = COMPONENT_SESSION_RUNTIME,
            event = "session_stdout_owner_renewer_stop_timeout",
            execution_id,
            stdout_owner_id = %ctx.stdout_owner_id,
            "terminal execution's stdout owner renewer did not stop promptly"
        );
    }
}

async fn record_max_duration_failure(
    ctx: &RuntimeContext,
    thread_key: &ThreadKey,
    execution_id: &str,
    max_duration: Duration,
    idle_timeout: Option<Duration>,
) -> Result<(), SessionRuntimeError> {
    let max_duration_ms = duration_millis_u64(max_duration);
    let error = format!("execution exceeded max_duration_ms={max_duration_ms}");
    let payload = json!({
        "execution_id": execution_id,
        "thread_key": thread_key.as_str(),
        "error": error,
        "reason": "max_duration_exceeded",
        "max_duration_ms": max_duration_ms,
    });
    let Some((execution, _)) = ctx
        .store
        .terminalize_execution_and_append_event_if_stdout_owner(
            execution_id,
            &ctx.stdout_owner_id,
            OwnedTerminalEvent::Failed { error, payload },
        )
        .await?
    else {
        return Ok(());
    };
    stop_terminal_stdout_owner_renewer(ctx, execution_id).await;
    ctx.execution_spans.lock().await.remove(execution_id);
    if let Err(error) = ctx.store.touch_session_sandbox_activity(thread_key).await {
        warn!(
            component = COMPONENT_SESSION_RUNTIME,
            event = "session_sandbox_activity_touch_failed",
            thread_key = %thread_key,
            execution_id,
            %error,
            "failed to touch sandbox activity after max duration"
        );
    }
    record_finished_execution_metric(
        &ctx.store,
        thread_key,
        &execution,
        "failed",
        Some("timeout"),
    )
    .await;
    if let Some(idle_timeout) = idle_timeout.or_else(|| idle_timeout_from_execution(&execution))
        && let Some(sandbox_id) = ctx.store.get_session(thread_key).await?.sandbox_id
    {
        spawn_idle_pause(
            ctx.clone(),
            thread_key.clone(),
            execution_id.to_owned(),
            sandbox_id,
            idle_timeout,
        );
    }
    Ok(())
}

fn spawn_idle_pause(
    ctx: RuntimeContext,
    thread_key: ThreadKey,
    execution_id: String,
    sandbox_id: String,
    idle_timeout: Duration,
) {
    tokio::spawn(async move {
        sleep(idle_timeout).await;
        if let Err(error) =
            record_idle_pause(&ctx, &thread_key, &execution_id, &sandbox_id, idle_timeout).await
        {
            warn!(%thread_key, %execution_id, %sandbox_id, %error, "idle pause task failed");
        }
    });
}

async fn record_idle_pause(
    ctx: &RuntimeContext,
    thread_key: &ThreadKey,
    execution_id: &str,
    sandbox_id: &str,
    idle_timeout: Duration,
) -> Result<(), SessionRuntimeError> {
    let latest_execution = ctx.store.latest_execution_for_thread(thread_key).await?;
    let session = ctx.store.get_session(thread_key).await?;
    if !should_pause_idle_sandbox(
        &session,
        latest_execution.as_ref(),
        execution_id,
        sandbox_id,
    ) {
        return Ok(());
    }

    let id = SandboxId::new(sandbox_id);
    match ctx.manager.status(&id).await {
        Ok(SandboxStatus::Suspended | SandboxStatus::Stopped | SandboxStatus::Gone) => {
            return Ok(());
        }
        Ok(SandboxStatus::Running | SandboxStatus::Created) => {}
        Ok(SandboxStatus::Unknown(_)) => return Ok(()),
        Err(SandboxError::NotFound(_)) => return Ok(()),
        Err(error) => {
            record_idle_pause_failure(
                &ctx.store,
                thread_key,
                execution_id,
                sandbox_id,
                idle_timeout,
                &error.to_string(),
            )
            .await?;
            return Err(SessionRuntimeError::Sandbox(error));
        }
    }

    let Some(mut assignment_lock) = ctx
        .store
        .lock_sandbox_assignment_for_reconciliation(thread_key, sandbox_id)
        .await?
    else {
        return Ok(());
    };
    let (Some(resource_uid), Some(assignment_epoch)) = (
        assignment_lock.resource_uid().map(str::to_owned),
        assignment_lock.assignment_epoch().map(str::to_owned),
    ) else {
        assignment_lock.rollback().await?;
        return Ok(());
    };
    if !assignment_lock
        .is_idle_after_execution(execution_id, idle_timeout)
        .await?
    {
        assignment_lock.rollback().await?;
        return Ok(());
    }
    match observe_assignment_reconciliation(&ctx.manager, &id).await {
        Ok(observed) if observed.resource_uid.as_deref() == Some(resource_uid.as_str()) => {}
        Ok(_) | Err(SandboxError::NotFound(_)) => {
            assignment_lock.rollback().await?;
            return Ok(());
        }
        Err(error) => {
            assignment_lock.rollback().await?;
            return Err(SessionRuntimeError::Sandbox(error));
        }
    }
    match pause_assignment_reconciliation(&ctx.manager, &id, &resource_uid).await {
        Ok(()) => {
            if !assignment_lock.commit_if_current().await? {
                return Ok(());
            }
            remove_pipe_for_assignment(
                &ctx.sandbox_pipes,
                sandbox_id,
                &resource_uid,
                &assignment_epoch,
            );
            ctx.store
                .append_event(
                    thread_key,
                    Some(execution_id),
                    "session.sandbox_paused",
                    json!({
                        "execution_id": execution_id,
                        "thread_key": thread_key.as_str(),
                        "sandbox_id": sandbox_id,
                        "reason": "idle_timeout",
                        "idle_timeout_ms": duration_millis_u64(idle_timeout),
                    }),
                )
                .await?;
        }
        Err(error) => {
            assignment_lock.rollback().await?;
            record_idle_pause_failure(
                &ctx.store,
                thread_key,
                execution_id,
                sandbox_id,
                idle_timeout,
                &error.to_string(),
            )
            .await?;
            return Err(SessionRuntimeError::Sandbox(error));
        }
    }
    Ok(())
}

async fn record_idle_pause_failure(
    store: &PgSessionStore,
    thread_key: &ThreadKey,
    execution_id: &str,
    sandbox_id: &str,
    idle_timeout: Duration,
    error: &str,
) -> Result<(), SessionRuntimeError> {
    store
        .append_event(
            thread_key,
            Some(execution_id),
            "session.sandbox_pause_failed",
            json!({
                "execution_id": execution_id,
                "thread_key": thread_key.as_str(),
                "sandbox_id": sandbox_id,
                "reason": "idle_timeout",
                "idle_timeout_ms": duration_millis_u64(idle_timeout),
                "error": error,
            }),
        )
        .await?;
    Ok(())
}

fn should_pause_idle_sandbox(
    session: &Session,
    latest_execution: Option<&SessionExecution>,
    execution_id: &str,
    sandbox_id: &str,
) -> bool {
    if session.sandbox_id.as_deref() != Some(sandbox_id) {
        return false;
    }
    let Some(execution) = latest_execution else {
        return false;
    };
    if execution.execution_id != execution_id {
        return false;
    }
    matches!(
        execution.status,
        ExecutionStatus::Completed | ExecutionStatus::Failed | ExecutionStatus::Cancelled
    )
}

fn duration_millis_u64(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn clean_persona_id(value: &str) -> Option<&str> {
    let value = value.trim();
    if value.is_empty() { None } else { Some(value) }
}

fn upsert_spec_env(spec: &mut SandboxSpec, name: &str, value: String) {
    if let Some(existing) = spec.env.iter_mut().find(|env| env.name == name) {
        existing.value = value;
    } else {
        spec.env
            .push(centaur_sandbox_core::EnvVar::new(name, value));
    }
}

fn sandbox_capabilities_match(
    existing: Option<&SessionSandboxCapabilities>,
    desired: &SessionSandboxCapabilities,
) -> bool {
    existing.map_or_else(
        || desired.is_default_enabled(),
        |existing| existing == desired,
    )
}

fn is_deleted_principal_error(error: &centaur_iron_control::IronControlError) -> bool {
    matches!(
        error,
        centaur_iron_control::IronControlError::Status { status: 404, .. }
    )
}

fn session_has_expired_metadata_trace_consent(session: &Session, now: OffsetDateTime) -> bool {
    session
        .sandbox_capabilities
        .as_ref()
        .is_some_and(|capabilities| {
            capabilities.metadata_trace_enabled
                && capabilities
                    .metadata_trace_expires_at
                    .is_none_or(|expires_at| expires_at <= now)
        })
}

fn sandbox_repo_cache_access_from_principal(
    principal: &centaur_iron_control::Principal,
) -> SessionRepoCacheAccess {
    match principal
        .labels
        .get(SANDBOX_REPO_CACHE_LABEL)
        .map(|value| value.trim().to_ascii_lowercase())
    {
        Some(value) if value == "all" => SessionRepoCacheAccess::All,
        Some(value) if value == "public" => SessionRepoCacheAccess::Public,
        Some(_) => SessionRepoCacheAccess::None,
        None => SessionRepoCacheAccess::None,
    }
}

fn sandbox_capabilities_from_principal(
    principal: &centaur_iron_control::Principal,
) -> SessionSandboxCapabilities {
    SessionSandboxCapabilities {
        repo_cache: sandbox_repo_cache_access_from_principal(principal),
        observability_enabled: principal.sandbox_observability_enabled,
        api_server_enabled: principal.sandbox_api_server_enabled,
        metadata_trace_enabled: false,
        metadata_trace_expires_at: None,
        metadata_trace_subject_hash: None,
        metadata_trace_consent_revision: None,
        metadata_trace_config_fingerprint: None,
        metadata_trace_config_generation: None,
    }
}

fn sandbox_capabilities_with_trace_subject(
    mut capabilities: SessionSandboxCapabilities,
    subject: &SlackTraceSubject,
    consent: &MetadataTraceConsent,
    metadata_trace_config: Option<&MetadataTraceConfigIdentity>,
) -> SessionSandboxCapabilities {
    let now = OffsetDateTime::now_utc();
    let metadata_trace_expires_at = consent
        .expires_at
        .filter(|expires_at| *expires_at > now && *expires_at <= now + MAX_METADATA_TRACE_CONSENT);
    let metadata_trace_enabled = metadata_trace_expires_at.is_some()
        && metadata_trace_config.is_some_and(|config| config.enabled);
    capabilities.metadata_trace_enabled =
        metadata_trace_enabled && consent.enabled && consent.revision > 0;
    capabilities.metadata_trace_expires_at = capabilities
        .metadata_trace_enabled
        .then_some(metadata_trace_expires_at)
        .flatten();
    capabilities.metadata_trace_subject_hash = capabilities
        .metadata_trace_enabled
        .then(|| trace_subject_hash(subject));
    capabilities.metadata_trace_consent_revision = capabilities
        .metadata_trace_enabled
        .then_some(consent.revision);
    capabilities.metadata_trace_config_fingerprint =
        capabilities.metadata_trace_enabled.then(|| {
            metadata_trace_config
                .expect("checked above")
                .fingerprint
                .clone()
        });
    capabilities.metadata_trace_config_generation = capabilities
        .metadata_trace_enabled
        .then(|| metadata_trace_config.expect("checked above").generation);
    capabilities
}

fn disable_metadata_trace(capabilities: &mut SessionSandboxCapabilities) {
    capabilities.metadata_trace_enabled = false;
    capabilities.metadata_trace_expires_at = None;
    capabilities.metadata_trace_subject_hash = None;
    capabilities.metadata_trace_consent_revision = None;
    capabilities.metadata_trace_config_fingerprint = None;
    capabilities.metadata_trace_config_generation = None;
}

fn metadata_trace_execution_boundary(capabilities: &SessionSandboxCapabilities) -> Value {
    json!({
        "metadata_trace_subject_hash": capabilities.metadata_trace_subject_hash,
        "metadata_trace_consent_revision": capabilities.metadata_trace_consent_revision,
        "metadata_trace_expires_at": capabilities.metadata_trace_expires_at.map(|value| value.to_string()),
        "metadata_trace_enabled": capabilities.metadata_trace_enabled,
        "metadata_trace_config_fingerprint": capabilities.metadata_trace_config_fingerprint,
        "metadata_trace_config_generation": capabilities.metadata_trace_config_generation,
    })
}

fn input_delivery_boundary_fingerprint(
    thread_key: &ThreadKey,
    metadata: Option<&Value>,
    capabilities: &SessionSandboxCapabilities,
) -> String {
    let actor_subject_hash =
        SlackTraceSubject::from_execution_metadata(thread_key.as_str(), metadata)
            .map(|subject| trace_subject_hash(&subject));
    let boundary = json!({
        "actor_subject_hash": actor_subject_hash,
        "metadata_trace_enabled": capabilities.metadata_trace_enabled,
        "metadata_trace_subject_hash": capabilities.metadata_trace_subject_hash,
        "metadata_trace_consent_revision": capabilities.metadata_trace_consent_revision,
        "metadata_trace_expires_at": capabilities.metadata_trace_expires_at.map(|value| value.to_string()),
        "metadata_trace_config_fingerprint": capabilities.metadata_trace_config_fingerprint,
        "metadata_trace_config_generation": capabilities.metadata_trace_config_generation,
    });
    let digest =
        Sha256::digest(serde_json::to_vec(&boundary).expect("input delivery boundary serializes"));
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn messages_match_active_trace_subject(
    thread_key: &ThreadKey,
    active: Option<&SessionExecution>,
    messages: &[SessionMessageInput],
) -> bool {
    let Some(active) = active else {
        return false;
    };
    let traced = active
        .metadata
        .get("metadata_trace_enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let Some(expected) = active
        .metadata
        .get("metadata_trace_subject_hash")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    else {
        // A traced execution without its durable subject hash is never an
        // unconstrained wildcard.  Retire it before accepting another input.
        return !traced;
    };
    messages
        .iter()
        .filter(|message| matches!(message.role, MessageRole::User))
        .all(|message| {
            SlackTraceSubject::from_execution_metadata(thread_key.as_str(), Some(&message.metadata))
                .is_some_and(|subject| trace_subject_hash(&subject) == expected)
        })
}

fn execution_trace_boundary_matches_capabilities(
    execution: &SessionExecution,
    capabilities: &SessionSandboxCapabilities,
) -> bool {
    let expected = metadata_trace_execution_boundary(capabilities);
    [
        "metadata_trace_subject_hash",
        "metadata_trace_consent_revision",
        "metadata_trace_expires_at",
        "metadata_trace_enabled",
        "metadata_trace_config_fingerprint",
        "metadata_trace_config_generation",
    ]
    .into_iter()
    .all(|key| execution.metadata.get(key) == expected.get(key))
}

fn trace_subject_hash(subject: &SlackTraceSubject) -> String {
    let digest = Sha256::digest(subject.stable_key().as_bytes());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
fn metadata_trace_write_timeout(remaining: Duration) -> Duration {
    #[cfg(test)]
    let max = Duration::from_millis(METADATA_TRACE_INPUT_TEST_TIMEOUT_MS.load(Ordering::Relaxed));
    #[cfg(not(test))]
    let max = METADATA_TRACE_INPUT_WRITE_MAX;
    remaining.min(max)
}

fn apply_sandbox_capabilities(spec: &mut SandboxSpec, capabilities: &SessionSandboxCapabilities) {
    let codex = spec
        .labels
        .get("centaur.ai/harness")
        .is_some_and(|harness| harness == "codex");
    let metadata_trace_enabled = capabilities.metadata_trace_enabled && codex;
    spec.capabilities = BackendSandboxCapabilities {
        repo_cache: match capabilities.repo_cache {
            SessionRepoCacheAccess::None => RepoCacheAccess::None,
            SessionRepoCacheAccess::Public => RepoCacheAccess::Public,
            SessionRepoCacheAccess::All => RepoCacheAccess::All,
        },
        observability_enabled: capabilities.observability_enabled,
        api_server_enabled: capabilities.api_server_enabled,
        metadata_trace_enabled,
    };
    upsert_spec_env(
        spec,
        "CENTAUR_SANDBOX_REPO_CACHE_ENABLED",
        capabilities.repo_cache_enabled().to_string(),
    );
    upsert_spec_env(
        spec,
        "CENTAUR_SANDBOX_REPO_CACHE_ACCESS",
        capabilities.repo_cache.as_str().to_owned(),
    );
    upsert_spec_env(
        spec,
        "CENTAUR_SANDBOX_OBSERVABILITY_ENABLED",
        capabilities.observability_enabled.to_string(),
    );
    upsert_spec_env(
        spec,
        "CENTAUR_SANDBOX_API_SERVER_ENABLED",
        capabilities.api_server_enabled.to_string(),
    );
    upsert_spec_env(
        spec,
        "CENTAUR_SANDBOX_METADATA_TRACE_ENABLED",
        metadata_trace_enabled.to_string(),
    );
    if metadata_trace_enabled {
        if let Some(expires_at) = capabilities.metadata_trace_expires_at {
            upsert_spec_env(
                spec,
                "CENTAUR_METADATA_TRACE_CONSENT_EXPIRES_AT_UNIX",
                expires_at.unix_timestamp().to_string(),
            );
        } else {
            // This should be unreachable for a resolved consent, but a trace
            // sidecar without a self-enforced deadline is not an acceptable
            // fallback.
            spec.capabilities.metadata_trace_enabled = false;
            remove_spec_env(spec, "CENTAUR_SANDBOX_METADATA_TRACE_ENABLED");
            upsert_spec_env(
                spec,
                "CENTAUR_SANDBOX_METADATA_TRACE_ENABLED",
                "false".to_owned(),
            );
            remove_spec_env(spec, "CENTAUR_METADATA_TRACE_CONSENT_EXPIRES_AT_UNIX");
        }
    } else {
        remove_spec_env(spec, "CENTAUR_METADATA_TRACE_CONSENT_EXPIRES_AT_UNIX");
    }
    if !codex {
        // Operator templates are shared across harnesses. Non-Codex
        // sandboxes must not inherit any OpenTelemetry control surface,
        // including variables outside the common OTLP exporter subset.
        spec.env.retain(|env| !env.name.starts_with("OTEL_"));
    }
    match capabilities.repo_cache {
        SessionRepoCacheAccess::None => {
            spec.mounts
                .retain(|mount| mount.target_path != SANDBOX_REPOS_MOUNT_PATH);
            remove_spec_env(spec, CENTAUR_SKILL_DIRS_ENV);
        }
        SessionRepoCacheAccess::Public => {
            scope_repo_cache_mounts_to_public(spec);
            scope_skill_dirs_to_public(spec);
        }
        SessionRepoCacheAccess::All => {
            remove_spec_env(spec, CENTAUR_PUBLIC_SKILL_DIRS_ENV);
        }
    }
    remove_spec_env(spec, CENTAUR_PUBLIC_SKILL_DIRS_ENV);
    if !capabilities.observability_enabled {
        append_spec_env_csv(spec, "TOOL_BLOCKLIST", OBSERVABILITY_TOOL_BLOCKLIST);
    }
}

fn scope_repo_cache_mounts_to_public(spec: &mut SandboxSpec) {
    for mount in spec
        .mounts
        .iter_mut()
        .filter(|mount| mount.target_path == SANDBOX_REPOS_MOUNT_PATH)
    {
        match &mut mount.kind {
            centaur_sandbox_core::MountKind::Bind { source_path } => {
                *source_path = format!(
                    "{}/{}",
                    source_path.trim_end_matches('/'),
                    PUBLIC_REPO_CACHE_SUBPATH
                );
            }
            centaur_sandbox_core::MountKind::NamedVolume(_) => {
                mount.sub_path = Some(PUBLIC_REPO_CACHE_SUBPATH.to_owned());
            }
            centaur_sandbox_core::MountKind::EmptyDir => {}
        }
    }
}

fn scope_skill_dirs_to_public(spec: &mut SandboxSpec) {
    let public_skill_dirs = spec
        .env
        .iter()
        .find(|env| env.name == CENTAUR_PUBLIC_SKILL_DIRS_ENV)
        .map(|env| env.value.trim().to_owned())
        .filter(|value| !value.is_empty());
    match public_skill_dirs {
        Some(public_skill_dirs) => upsert_spec_env(spec, CENTAUR_SKILL_DIRS_ENV, public_skill_dirs),
        None => remove_spec_env(spec, CENTAUR_SKILL_DIRS_ENV),
    }
}

fn append_spec_env_csv(spec: &mut SandboxSpec, name: &str, values: &str) {
    let existing = spec
        .env
        .iter()
        .find(|env| env.name == name)
        .map(|env| env.value.as_str())
        .unwrap_or("");
    let mut merged = existing
        .split(',')
        .chain(values.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .fold(Vec::<String>::new(), |mut acc, value| {
            if !acc.iter().any(|existing| existing == value) {
                acc.push(value.to_owned());
            }
            acc
        });
    merged.sort();
    upsert_spec_env(spec, name, merged.join(","));
}

fn apply_persona_spec_env(mut spec: SandboxSpec, persona: Option<&PersonaContext>) -> SandboxSpec {
    for name in [
        "AGENT_PERSONA",
        "CENTAUR_PERSONA_ID",
        "CENTAUR_PERSONA_PROMPT_HASH",
        "CENTAUR_PERSONA_SOURCE_PATH",
        "CENTAUR_PERSONA_SOURCE_REF",
    ] {
        remove_spec_env(&mut spec, name);
    }
    let Some(persona) = persona else {
        return spec;
    };
    upsert_spec_env(&mut spec, "AGENT_PERSONA", persona.persona_id.clone());
    upsert_spec_env(&mut spec, "CENTAUR_PERSONA_ID", persona.persona_id.clone());
    upsert_spec_env(
        &mut spec,
        "CENTAUR_PERSONA_PROMPT_HASH",
        persona.prompt_hash.clone(),
    );
    upsert_spec_env(
        &mut spec,
        "CENTAUR_PERSONA_SOURCE_PATH",
        persona.source_path.clone(),
    );
    if let Some(source_ref) = persona.source_ref.as_ref() {
        upsert_spec_env(&mut spec, "CENTAUR_PERSONA_SOURCE_REF", source_ref.clone());
    }
    spec
}

fn remove_spec_env(spec: &mut SandboxSpec, name: &str) {
    spec.env.retain(|env| env.name != name);
}

fn add_persona_metadata(metadata: &mut Value, context: &PersonaContext) {
    if let Value::Object(object) = metadata {
        object.insert("persona".to_owned(), json!(context));
    }
}

async fn record_finished_execution_metric(
    store: &PgSessionStore,
    thread_key: &ThreadKey,
    execution: &SessionExecution,
    status: &'static str,
    failure_class: Option<&'static str>,
) {
    let harness_label = match store.get_session(thread_key).await {
        Ok(session) => session.harness_type.to_string(),
        Err(error) => {
            warn!(%thread_key, %error, "failed to load session for execution metric labels");
            "unknown".to_owned()
        }
    };
    record_session_execution_finished(&harness_label, status, execution_duration(execution));
    if let Some(failure_class) = failure_class {
        record_session_failure(&harness_label, failure_class);
    }
}

fn execution_duration(execution: &SessionExecution) -> Option<Duration> {
    let started_at = execution.started_at.unwrap_or(execution.created_at);
    let completed_at = execution.completed_at?;
    (completed_at - started_at).try_into().ok()
}

fn runtime_error_failure_class(error: &SessionRuntimeError) -> &'static str {
    match error {
        SessionRuntimeError::BadRequest(_) => "bad_request",
        SessionRuntimeError::MetadataTraceBoundaryChanged => "metadata_trace_boundary_changed",
        SessionRuntimeError::InactiveMetadataTraceConfig => "metadata_trace_config_inactive",
        SessionRuntimeError::SandboxAssignmentChanged => "sandbox_assignment_changed",
        SessionRuntimeError::ShuttingDown => "shutting_down",
        SessionRuntimeError::StdoutOwnerRenewerStopTimeout { .. } => "stdout_owner",
        SessionRuntimeError::Store(_) => "store",
        SessionRuntimeError::Sandbox(SandboxError::NotFound(_)) => "sandbox_not_found",
        SessionRuntimeError::Sandbox(SandboxError::Unsupported { .. }) => "sandbox_unsupported",
        SessionRuntimeError::Sandbox(SandboxError::NotReady(_)) => "sandbox_not_ready",
        SessionRuntimeError::Sandbox(SandboxError::Io { .. }) => "sandbox_io",
        SessionRuntimeError::Sandbox(SandboxError::Backend { .. }) => "sandbox_backend",
        SessionRuntimeError::Sandbox(SandboxError::InvalidSpec(_)) => "sandbox_invalid_spec",
        SessionRuntimeError::IronControl(_) => "iron_control",
        SessionRuntimeError::WarmPool(_) => "warm_pool",
        SessionRuntimeError::CapacityExceeded { .. } => "capacity",
    }
}

fn terminal_failure_class(error: &str) -> &'static str {
    let error = error.to_ascii_lowercase();
    if error.contains("max_duration") || error.contains("timeout") || error.contains("timed out") {
        return "timeout";
    }
    if error.contains("execution orphaned") {
        return "orphaned";
    }
    if error.contains("sandbox stdout") || error.contains("stdout closed") {
        return "sandbox_io";
    }
    "harness"
}

fn should_attach_session_pipe(status: &SandboxStatus) -> bool {
    status.can_open_io()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExistingSandboxAction {
    Reuse,
    ResumeOrReplace,
    Replace,
}

fn existing_sandbox_action(status: &SandboxStatus) -> ExistingSandboxAction {
    match status {
        SandboxStatus::Running => ExistingSandboxAction::Reuse,
        SandboxStatus::Created | SandboxStatus::Suspended => ExistingSandboxAction::ResumeOrReplace,
        SandboxStatus::Stopped | SandboxStatus::Gone | SandboxStatus::Unknown(_) => {
            ExistingSandboxAction::Replace
        }
    }
}

fn is_event_stream_attach_race(error: &SessionRuntimeError) -> bool {
    matches!(
        error,
        SessionRuntimeError::Sandbox(SandboxError::NotReady(_))
    )
}

fn terminal_output(value: &Value, prior_final_answer_text: &str) -> Option<TerminalOutput> {
    let method = value.get("method").and_then(Value::as_str);
    let event_type = value.get("type").and_then(Value::as_str);

    if event_type == Some("run.failed")
        && matches!(
            value.pointer("/payload/status").and_then(Value::as_str),
            Some("cancelled" | "canceled")
        )
    {
        return Some(TerminalOutput::Cancelled {
            reason: "turn_interrupted",
        });
    }

    if matches!(method, Some("error" | "turn/failed"))
        || matches!(event_type, Some("error" | "turn.failed" | "run.failed"))
    {
        return Some(TerminalOutput::Failed {
            error: terminal_error_text(value),
        });
    }

    if method == Some("turn/completed") {
        return Some(completed_turn_terminal_output(
            value,
            prior_final_answer_text,
        ));
    }

    match event_type {
        Some("run.completed") => Some(completed_terminal_output_with_fallback(
            value,
            "run_completed",
            prior_final_answer_text,
        )),
        Some("turn.completed") => Some(completed_turn_terminal_output(
            value,
            prior_final_answer_text,
        )),
        Some("turn.done") => Some(completed_terminal_output(value, "turn_done")),
        Some("result") => {
            if result_is_failure(value) {
                Some(TerminalOutput::Failed {
                    error: terminal_error_text(value),
                })
            } else {
                Some(completed_terminal_output(value, "result"))
            }
        }
        _ => None,
    }
}

fn completed_turn_terminal_output(value: &Value, prior_final_answer_text: &str) -> TerminalOutput {
    match turn_completion_status(value).as_deref() {
        Some("completed" | "succeeded" | "success") | None => {
            completed_terminal_output_with_fallback(
                value,
                "turn_completed",
                prior_final_answer_text,
            )
        }
        Some("interrupted") if prior_final_answer_text.trim().is_empty() => {
            TerminalOutput::Cancelled {
                reason: "turn_interrupted",
            }
        }
        Some(_status) if !prior_final_answer_text.trim().is_empty() => {
            completed_terminal_output_with_fallback(
                value,
                "turn_completed",
                prior_final_answer_text,
            )
        }
        Some(status) => TerminalOutput::Failed {
            error: format!("turn completed with status {status} before final answer"),
        },
    }
}

fn completed_terminal_output(value: &Value, reason: &'static str) -> TerminalOutput {
    completed_terminal_output_with_fallback(value, reason, "")
}

fn completed_terminal_output_with_fallback(
    value: &Value,
    reason: &'static str,
    fallback_text: &str,
) -> TerminalOutput {
    let result_text = terminal_payload_text(value).trim().to_owned();
    let result_text = if result_text.is_empty() {
        fallback_text.to_owned()
    } else {
        result_text
    };
    TerminalOutput::Completed {
        reason,
        result_text: (!result_text.is_empty()).then_some(result_text),
    }
}

fn turn_completion_status(value: &Value) -> Option<String> {
    [
        &["turn", "status"][..],
        &["params", "turn", "status"][..],
        &["status"][..],
        &["params", "status"][..],
    ]
    .into_iter()
    .filter_map(|path| string_at_path(value, path))
    .next()
}

enum FinalAnswerTextUpdate {
    Append(String),
    Replace(String),
}

fn output_line_final_answer_text(value: &Value) -> Option<FinalAnswerTextUpdate> {
    let method = value.get("method").and_then(Value::as_str);
    let event_type = value.get("type").and_then(Value::as_str);
    if event_type == Some("assistant.delta") {
        if nanocodex_message_phase(value) == Some("commentary") {
            return None;
        }
        let text = value
            .get("payload")
            .and_then(|payload| payload.get("text"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        return (!text.is_empty()).then_some(FinalAnswerTextUpdate::Append(text));
    }
    if event_type == Some("assistant.message") {
        if nanocodex_message_phase(value) == Some("commentary") {
            return None;
        }
        let text = value
            .get("payload")
            .and_then(|payload| payload.get("text"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        return (!text.is_empty()).then_some(FinalAnswerTextUpdate::Replace(text));
    }
    if matches!(method, Some("item/agentMessage/delta"))
        || matches!(event_type, Some("item.agentMessage.delta"))
    {
        let text = [
            value.get("delta"),
            value.get("params").and_then(|params| params.get("delta")),
            value
                .get("payload")
                .and_then(|payload| payload.get("delta")),
        ]
        .into_iter()
        .flatten()
        .find_map(Value::as_str)
        .unwrap_or_default()
        .to_owned();
        return (!text.is_empty()).then_some(FinalAnswerTextUpdate::Append(text));
    }
    if event_type == Some("assistant") {
        let text = terminal_payload_text(value).trim().to_owned();
        return (!text.is_empty()).then_some(FinalAnswerTextUpdate::Replace(text));
    }
    if matches!(method, Some("item/completed")) || matches!(event_type, Some("item.completed")) {
        let item = value
            .get("item")
            .or_else(|| value.get("params").and_then(|params| params.get("item")));
        if let Some(item) = item
            && matches!(
                item.get("type").and_then(Value::as_str),
                Some("agentMessage" | "agent_message")
            )
            && matches!(
                item.get("phase").and_then(Value::as_str),
                Some("final_answer" | "answer") | None
            )
        {
            let text = terminal_payload_text(item).trim().to_owned();
            return (!text.is_empty()).then_some(FinalAnswerTextUpdate::Replace(text));
        }
    }
    None
}

fn nanocodex_message_phase(value: &Value) -> Option<&str> {
    value
        .get("payload")
        .and_then(|payload| payload.get("phase"))
        .and_then(Value::as_str)
}

fn turn_ids(value: &Value) -> Vec<String> {
    [
        &["turn_id"][..],
        &["turnId"][..],
        &["turn", "id"][..],
        &["params", "turnId"][..],
        &["params", "turn", "id"][..],
    ]
    .into_iter()
    .filter_map(|path| string_at_path(value, path))
    .collect()
}

fn item_ids(value: &Value) -> Vec<String> {
    [
        &["item_id"][..],
        &["itemId"][..],
        &["item", "id"][..],
        &["params", "itemId"][..],
        &["params", "item", "id"][..],
    ]
    .into_iter()
    .filter_map(|path| string_at_path(value, path))
    .collect()
}

fn string_at_path(value: &Value, path: &[&str]) -> Option<String> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    let text = current.as_str()?.trim();
    (!text.is_empty()).then(|| text.to_owned())
}

fn result_is_failure(value: &Value) -> bool {
    matches!(
        value.get("subtype").and_then(Value::as_str),
        Some("error" | "failure" | "failed")
    )
}

fn terminal_error_text(value: &Value) -> String {
    for key in ["error", "message", "result", "text"] {
        if let Some(text) = value.get(key).and_then(Value::as_str)
            && !text.trim().is_empty()
        {
            return text.trim().to_owned();
        }
    }
    terminal_payload_text(value)
        .trim()
        .to_owned()
        .if_empty("terminal harness output reported failure")
}

fn terminal_payload_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Array(values) => values
            .iter()
            .map(terminal_payload_text)
            .find(|text| !text.trim().is_empty())
            .unwrap_or_default(),
        Value::Object(object) => {
            for key in [
                "result",
                "result_text",
                "text",
                "final_text",
                "message",
                "delta",
                "content",
                "params",
                "payload",
            ] {
                if let Some(text) = object.get(key).map(terminal_payload_text)
                    && !text.trim().is_empty()
                {
                    return text;
                }
            }
            String::new()
        }
        _ => String::new(),
    }
}

trait StringExt {
    fn if_empty(self, fallback: &str) -> String;
}

impl StringExt for String {
    fn if_empty(self, fallback: &str) -> String {
        if self.is_empty() {
            fallback.to_owned()
        } else {
            self
        }
    }
}

async fn drain_stderr(mut stderr: SandboxRead) -> Result<(), SessionRuntimeError> {
    io::copy(&mut stderr, &mut io::sink())
        .await
        .map_err(|err| {
            SessionRuntimeError::Sandbox(SandboxError::io_source("drain stderr", err))
        })?;
    Ok(())
}

/// Trace identity injected into sandbox stdin lines so the Rust harness server
/// can configure the harness OTLP export. Without a `trace_id` or `traceparent`
/// on the first turn, Codex exports no `session_task.turn` spans and Laminar
/// has no token usage to price into cost.
#[derive(Clone, Debug)]
struct SessionTraceContext {
    /// Stable per-thread trace id, derived from the thread key (UUIDv5) so it
    /// needs no persisted state and survives API restarts.
    trace_id: String,
    /// W3C traceparent of the current execution span, when the OpenTelemetry
    /// layer is active. Lets harness spans join the execution's trace.
    traceparent: Option<String>,
}

impl SessionTraceContext {
    fn new(thread_key: &ThreadKey, execution_span: Option<&Span>) -> Self {
        Self {
            trace_id: thread_trace_id(thread_key),
            traceparent: execution_span.and_then(centaur_telemetry::traceparent_for_span),
        }
    }
}

/// Deterministic per-thread trace id: one trace identity per thread without a
/// `thread_traces` table (derive, don't store).
pub fn thread_trace_id(thread_key: &ThreadKey) -> String {
    uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_URL,
        format!("centaur:thread:{}", thread_key.as_str()).as_bytes(),
    )
    .to_string()
}

fn ensure_thread_trace_root_span(thread_key: &ThreadKey) {
    let trace_id = thread_trace_id(thread_key);
    let root_span_id = thread_trace_parent_span_id(thread_key);
    let thread_key = thread_key.as_str().to_owned();
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(async move {
            let _ = export_thread_trace_root_span(&trace_id, &root_span_id, &thread_key).await;
        });
    }
}

pub fn thread_trace_parent_span_id(thread_key: &ThreadKey) -> String {
    let digest = Sha256::digest(format!("centaur:thread-parent:{}", thread_key.as_str()));
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    if bytes.iter().all(|byte| *byte == 0) {
        bytes[7] = 1;
    }
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn input_lines_with_session_context(
    thread_key: &ThreadKey,
    trace: &SessionTraceContext,
    input_lines: &[String],
) -> Vec<String> {
    input_lines
        .iter()
        .map(|line| input_line_with_session_context(thread_key, trace, line))
        .collect()
}

fn input_line_with_session_context(
    thread_key: &ThreadKey,
    trace: &SessionTraceContext,
    line: &str,
) -> String {
    let Ok(mut value) = serde_json::from_str::<Value>(line) else {
        return line.to_owned();
    };
    let Value::Object(map) = &mut value else {
        return line.to_owned();
    };
    map.entry("thread_key")
        .or_insert_with(|| Value::String(thread_key.as_str().to_owned()));
    map.entry("trace_id")
        .or_insert_with(|| Value::String(trace.trace_id.clone()));
    if let Some(traceparent) = &trace.traceparent {
        map.entry("traceparent")
            .or_insert_with(|| Value::String(traceparent.clone()));
    }
    prepend_chat_surface_note(map, thread_key);
    merge_session_context(map, session_context_for_thread(thread_key));
    serde_json::to_string(&value).unwrap_or_else(|_| line.to_owned())
}

/// Prepend a terse chat-surface note to a user turn's content so the agent always
/// knows which platform (Slack/Discord) and destination it is operating on.
///
/// The static system prompt is platform-neutral, so this per-turn line is the
/// agent's authoritative signal for where its reply and uploads land. It is added
/// only to `user` turns whose content is an array of message parts and whose
/// thread key resolves to a known chat destination; every other shape is left
/// untouched.
fn prepend_chat_surface_note(map: &mut serde_json::Map<String, Value>, thread_key: &ThreadKey) {
    if map.get("type").and_then(Value::as_str) != Some("user") {
        return;
    }
    let Some(destination) = thread_key.chat_destination() else {
        return;
    };
    let Some(Value::Array(content)) = map.get_mut("message").and_then(|m| m.get_mut("content"))
    else {
        return;
    };
    content.insert(
        0,
        json!({ "type": "text", "text": destination.context_line() }),
    );
}

fn merge_session_context(
    map: &mut serde_json::Map<String, Value>,
    context: Option<serde_json::Map<String, Value>>,
) {
    let Some(context) = context else {
        return;
    };
    let entry = map
        .entry("session_context")
        .or_insert_with(|| Value::Object(serde_json::Map::new()));
    let Value::Object(existing) = entry else {
        return;
    };
    for (key, value) in context {
        existing.entry(key).or_insert(value);
    }
}

/// Build the structured per-turn session context for a thread, mirroring the
/// `/api/session` response shape (`{ platform, <slack|discord|linear|github>: { .. } }`).
///
/// Resolved from the same [`ChatDestination`] the session-context route uses, so
/// the structured context the agent sees in its input is consistent with what
/// tools read back from the API. Returns `None` for non-platform threads (e.g.
/// `api:` keys), which carry no chat destination and get no `session_context`.
fn session_context_for_thread(thread_key: &ThreadKey) -> Option<serde_json::Map<String, Value>> {
    let destination = thread_key.chat_destination()?;
    let mut context = serde_json::Map::new();
    context.insert(
        "platform".to_owned(),
        Value::String(destination.platform().to_owned()),
    );
    let (platform_key, block) = match destination {
        ChatDestination::Slack {
            channel_id,
            thread_ts,
        } => {
            let mut slack = serde_json::Map::new();
            slack.insert("channel_id".to_owned(), Value::String(channel_id));
            slack.insert("thread_ts".to_owned(), Value::String(thread_ts));
            ("slack", slack)
        }
        ChatDestination::Discord {
            guild_id,
            channel_id,
            thread_id,
        } => {
            let mut discord = serde_json::Map::new();
            discord.insert("guild_id".to_owned(), Value::String(guild_id));
            discord.insert("channel_id".to_owned(), Value::String(channel_id));
            if let Some(thread_id) = thread_id {
                discord.insert("thread_id".to_owned(), Value::String(thread_id));
            }
            ("discord", discord)
        }
        ChatDestination::Linear {
            issue_id,
            comment_id,
            agent_session_id,
        } => {
            let mut linear = serde_json::Map::new();
            linear.insert("issue_id".to_owned(), Value::String(issue_id));
            if let Some(comment_id) = comment_id {
                linear.insert("comment_id".to_owned(), Value::String(comment_id));
            }
            if let Some(agent_session_id) = agent_session_id {
                linear.insert(
                    "agent_session_id".to_owned(),
                    Value::String(agent_session_id),
                );
            }
            ("linear", linear)
        }
        ChatDestination::Github {
            owner,
            repo,
            number,
            kind,
            review_comment_id,
        } => {
            let mut github = serde_json::Map::new();
            github.insert("owner".to_owned(), Value::String(owner));
            github.insert("repo".to_owned(), Value::String(repo));
            github.insert("number".to_owned(), Value::Number(number.into()));
            github.insert("kind".to_owned(), Value::String(kind.as_str().to_owned()));
            if let Some(review_comment_id) = review_comment_id {
                github.insert(
                    "review_comment_id".to_owned(),
                    Value::Number(review_comment_id.into()),
                );
            }
            ("github", github)
        }
    };
    context.insert(platform_key.to_owned(), Value::Object(block));
    Some(context)
}

fn steering_input_lines(
    thread_key: &ThreadKey,
    messages: &[SessionMessageInput],
    message_ids: &[String],
) -> Vec<String> {
    messages
        .iter()
        .zip(message_ids)
        .filter_map(|(message, message_id)| steering_input_line(thread_key, message, message_id))
        .collect()
}

fn prepare_session_messages(
    thread_key: &ThreadKey,
    messages: &[SessionMessageInput],
) -> Vec<PreparedSessionMessage> {
    messages
        .iter()
        .map(|input| {
            let message_id = input.client_message_id.as_deref().map_or_else(
                || format!("msg_{}", Uuid::new_v4().simple()),
                |client_message_id| {
                    let stable = Uuid::new_v5(
                        &Uuid::NAMESPACE_URL,
                        format!("centaur:{thread_key}:{client_message_id}").as_bytes(),
                    );
                    format!("msg_{}", stable.simple())
                },
            );
            PreparedSessionMessage {
                message_id,
                input: input.clone(),
            }
        })
        .collect()
}

fn input_delivery_idempotency_key(
    thread_key: &ThreadKey,
    messages: &[PreparedSessionMessage],
) -> String {
    let mut hash = Sha256::new();
    hash.update(thread_key.as_str().as_bytes());
    for message in messages {
        hash.update([0]);
        hash.update(message.message_id.as_bytes());
    }
    let digest = hash.finalize();
    let encoded = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("messages:{encoded}")
}

fn steering_input_line(
    thread_key: &ThreadKey,
    message: &SessionMessageInput,
    message_id: &str,
) -> Option<String> {
    if message.role != MessageRole::User {
        return None;
    }
    serde_json::to_string(&json!({
        "type": "user",
        "thread_key": thread_key.as_str(),
        "trace_metadata": {
            "source": "session.append_messages",
            "action": "steer_active_execution",
            "message_id": message_id,
            "metadata": message.metadata.clone(),
        },
        "message": {
            "role": message.role.as_ref(),
            "content": message.parts.clone(),
        },
    }))
    .ok()
}

fn interrupt_input_line(thread_key: &ThreadKey, reason: &str) -> String {
    serde_json::to_string(&json!({
        "type": "interrupt",
        "thread_key": thread_key.as_str(),
        "trace_metadata": {
            "source": "session.interrupt_active_execution",
            "action": "interrupt_active_execution",
            "reason": reason,
        },
    }))
    .expect("interrupt input line serializes")
}

async fn append_output_line(
    ctx: &RuntimeContext,
    thread_key: &ThreadKey,
    execution_id: &str,
    line: &str,
) -> Result<Option<SessionEvent>, SessionRuntimeError> {
    let safe_line = redact_sensitive_text(line);
    let event = ctx
        .store
        .append_event_if_stdout_owner(
            thread_key,
            execution_id,
            &ctx.stdout_owner_id,
            STDOUT_OWNER_LEASE,
            SESSION_OUTPUT_LINE_EVENT,
            Value::String(safe_line),
        )
        .await?;
    Ok(event)
}

fn redact_sensitive_text(input: &str) -> String {
    let bearer_redacted = redact_bearer_tokens(input);
    let env_redacted = redact_sensitive_env_assignments(&bearer_redacted);
    redact_prefixed_tokens(&env_redacted)
}

fn redact_bearer_tokens(input: &str) -> String {
    const BEARER: &str = "bearer ";
    let lower = input.to_ascii_lowercase();
    let mut out = String::with_capacity(input.len());
    let mut index = 0;

    while let Some(relative) = lower[index..].find(BEARER) {
        let start = index + relative;
        let token_start = start + BEARER.len();
        let token_end = consume_sensitive_token(input, token_start);
        out.push_str(&input[index..token_start]);
        if token_end > token_start {
            out.push_str("[REDACTED_TOKEN]");
            index = token_end;
        } else {
            index = token_start;
        }
    }

    out.push_str(&input[index..]);
    out
}

fn redact_sensitive_env_assignments(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut index = 0;

    while let Some(relative) = input[index..].find('=') {
        let equals = index + relative;
        let key_start = env_key_start(input, equals);
        let key = &input[key_start..equals];
        out.push_str(&input[index..=equals]);
        if is_sensitive_env_key(key) {
            let token_start = equals + 1;
            let token_end = consume_sensitive_token(input, token_start);
            if token_end > token_start {
                out.push_str("[REDACTED_TOKEN]");
                index = token_end;
                continue;
            }
        }
        index = equals + 1;
    }

    out.push_str(&input[index..]);
    out
}

fn redact_prefixed_tokens(input: &str) -> String {
    const PREFIXES: &[&str] = &[
        "sbx1.",
        "xoxa-",
        "xoxb-",
        "xoxp-",
        "xoxr-",
        "xoxs-",
        "sk-ant-",
        "sk-",
        "ghp_",
        "gho_",
        "ghu_",
        "ghs_",
        "ghr_",
        "github_pat_",
    ];

    let mut out = String::with_capacity(input.len());
    let mut index = 0;
    while index < input.len() {
        if let Some(prefix) = PREFIXES
            .iter()
            .find(|prefix| should_redact_prefixed_token(input, index, prefix))
        {
            let token_end = consume_sensitive_token(input, index + prefix.len());
            out.push_str("[REDACTED_TOKEN]");
            index = token_end;
            continue;
        }

        let ch = input[index..].chars().next().expect("valid char boundary");
        out.push(ch);
        index += ch.len_utf8();
    }

    out
}

fn should_redact_prefixed_token(input: &str, index: usize, prefix: &str) -> bool {
    if !input[index..].starts_with(prefix) || !has_token_boundary_before(input, index) {
        return false;
    }

    let token_start = index + prefix.len();
    let token_end = consume_sensitive_token(input, token_start);
    if token_end == token_start {
        return false;
    }

    if prefix.starts_with("sk-") {
        return token_end.saturating_sub(token_start) >= 16;
    }

    true
}

fn has_token_boundary_before(input: &str, index: usize) -> bool {
    if index == 0 {
        return true;
    }

    input[..index]
        .chars()
        .next_back()
        .is_none_or(|ch| !is_sensitive_token_char(ch))
}

fn consume_sensitive_token(input: &str, start: usize) -> usize {
    let mut end = start;
    for (relative, ch) in input[start..].char_indices() {
        if !is_sensitive_token_char(ch) {
            break;
        }
        end = start + relative + ch.len_utf8();
    }
    end
}

fn is_sensitive_token_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '=' | '+' | '/' | '.' | ':')
}

fn env_key_start(input: &str, equals: usize) -> usize {
    let mut start = equals;
    for (index, ch) in input[..equals].char_indices().rev() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-') {
            start = index;
        } else {
            break;
        }
    }
    start
}

fn is_sensitive_env_key(key: &str) -> bool {
    let upper = key.to_ascii_uppercase();
    upper.contains("API_KEY")
        || upper.contains("TOKEN")
        || upper.contains("SECRET")
        || upper.contains("PASSWORD")
}

fn harness_thread_id_from_output_line(line: &str) -> Option<String> {
    let value: Value = serde_json::from_str(line).ok()?;
    root_thread_id_from_output(&value)
}

fn legacy_corrupted_root_repair_candidate(lines: &[String], durable_root: &str) -> Option<String> {
    let started_threads = lines
        .iter()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter_map(|value| thread_started_id_from_output(&value))
        .collect::<Vec<_>>();
    let candidate = started_threads.first()?.trim();
    if candidate.is_empty() || candidate == durable_root {
        return None;
    }
    started_threads
        .iter()
        .skip(1)
        .any(|thread_id| thread_id == durable_root)
        .then(|| candidate.to_owned())
}

fn thread_started_id_from_output(value: &Value) -> Option<String> {
    let method = value.get("method").and_then(Value::as_str);
    if method == Some("thread/started") {
        return string_at_path(value, &["params", "thread", "id"]);
    }
    let event_type = value.get("type").and_then(Value::as_str);
    if event_type != Some("thread.started") {
        return None;
    }
    value
        .get("thread_id")
        .or_else(|| value.get("threadId"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|thread_id| !thread_id.is_empty())
        .map(ToOwned::to_owned)
}

fn root_thread_id_from_output(value: &Value) -> Option<String> {
    let method = value.get("method").and_then(Value::as_str);
    let event_type = value.get("type").and_then(Value::as_str);
    if event_type == Some("run.started") {
        return value
            .get("request_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|request_id| !request_id.is_empty())
            .map(ToOwned::to_owned);
    }
    if method == Some("thread/started") {
        return thread_started_id_from_output(value);
    }
    if event_type != Some("thread.started") {
        return None;
    }
    thread_started_id_from_output(value)
}

fn output_request_id(value: &Value) -> Option<&str> {
    value
        .get("request_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|request_id| !request_id.is_empty())
}

fn output_belongs_to_thread(value: &Value, root_thread_id: &str) -> bool {
    if let Some(request_id) = output_request_id(value) {
        return request_id == root_thread_id;
    }
    let event_thread_id = [
        &["thread_id"][..],
        &["threadId"][..],
        &["params", "threadId"][..],
        &["params", "thread", "id"][..],
    ]
    .into_iter()
    .find_map(|path| string_at_path(value, path));
    event_thread_id.is_none_or(|thread_id| thread_id == root_thread_id)
}

fn validate_input_lines(lines: &[String]) -> Result<(), SessionRuntimeError> {
    for (index, line) in lines.iter().enumerate() {
        if line.contains('\n') || line.contains('\r') {
            return Err(SessionRuntimeError::BadRequest(format!(
                "input_lines[{index}] must be one line"
            )));
        }
    }
    Ok(())
}

fn stdout_pump_error_message(error: &LinesCodecError) -> String {
    match error {
        LinesCodecError::MaxLineLengthExceeded => {
            "sandbox stdout line exceeded codec maximum length".to_owned()
        }
        LinesCodecError::Io(error) => format!("sandbox stdout I/O failed: {error}"),
    }
}

fn codec_error_to_runtime(error: LinesCodecError) -> SessionRuntimeError {
    let context = error.to_string();
    SessionRuntimeError::Sandbox(SandboxError::Io {
        context,
        source: Some(Box::new(error)),
    })
}

fn duration_options(
    idle_timeout_ms: Option<u64>,
    max_duration_ms: Option<u64>,
) -> Result<(Option<Duration>, Option<Duration>), SessionRuntimeError> {
    let idle_timeout = idle_timeout_ms.map(nonzero_duration_millis).transpose()?;
    let max_duration = max_duration_ms.map(nonzero_duration_millis).transpose()?;

    if let (Some(idle_timeout), Some(max_duration)) = (idle_timeout, max_duration)
        && idle_timeout > max_duration
    {
        return Err(SessionRuntimeError::BadRequest(
            "idle_timeout_ms must be less than or equal to max_duration_ms".to_owned(),
        ));
    }

    Ok((idle_timeout, max_duration))
}

fn nonzero_duration_millis(value: u64) -> Result<Duration, SessionRuntimeError> {
    if value == 0 {
        return Err(SessionRuntimeError::BadRequest(
            "duration values must be greater than zero".to_owned(),
        ));
    }
    Ok(Duration::from_millis(value))
}

fn tool_host_thread_key(principal_id: &str) -> Result<ThreadKey, SessionRuntimeError> {
    ThreadKey::parse(format!("mcp:{principal_id}"))
        .map_err(|error| SessionRuntimeError::BadRequest(error.to_string()))
}

/// Session/principal metadata recorded for observability; runtime behavior
/// derives from the `mcp:` thread-key prefix, not from these fields.
fn tool_host_session_metadata(principal_id: &str) -> Value {
    json!({
        "mcp_tool_host": true,
        "mcp_principal_id": principal_id,
    })
}

fn proxy_labels_from_session_metadata(
    thread_key: &ThreadKey,
    metadata: &Value,
) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    insert_metadata_string_label(
        &mut labels,
        "centaur.slack_user_id",
        metadata.get("slack_user_id"),
    );
    insert_metadata_string_label(
        &mut labels,
        "centaur.slack_team_id",
        metadata.get("slack_team_id"),
    );
    insert_metadata_string_label(
        &mut labels,
        "centaur.slack_channel_id",
        metadata.get("slack_channel_id"),
    );
    if !labels.contains_key("centaur.slack_channel_id")
        && let Some(channel_id) = slack_conversation_id(thread_key)
    {
        labels.insert("centaur.slack_channel_id".to_owned(), channel_id.to_owned());
    }
    labels
}

fn insert_metadata_string_label(
    labels: &mut BTreeMap<String, String>,
    label: &str,
    value: Option<&Value>,
) {
    let Some(value) = value.and_then(Value::as_str).map(str::trim) else {
        return;
    };
    if !value.is_empty() {
        labels.insert(label.to_owned(), value.to_owned());
    }
}

fn slack_conversation_id(thread_key: &ThreadKey) -> Option<String> {
    if let Some(ChatDestination::Slack { channel_id, .. }) = thread_key.chat_destination() {
        return Some(channel_id);
    }
    None
}

fn sandbox_boot_mode_for_thread(
    thread_key: &ThreadKey,
    iron_control_principal: Option<&str>,
) -> SandboxBootMode {
    let Some(thread_principal_id) = thread_key.as_str().strip_prefix("mcp:") else {
        return SandboxBootMode::Harness;
    };
    let principal_id = iron_control_principal
        .unwrap_or(thread_principal_id)
        .to_owned();
    SandboxBootMode::ToolHost { principal_id }
}

fn apply_sandbox_boot_mode(spec: &mut SandboxSpec, boot_mode: &SandboxBootMode) {
    let SandboxBootMode::ToolHost { principal_id } = boot_mode else {
        return;
    };
    spec.labels
        .insert("centaur.ai/component".to_owned(), "tool-host".to_owned());
    spec.labels
        .insert("centaur.ai/workload".to_owned(), "mcp-tool-host".to_owned());
    if !principal_id.trim().is_empty() {
        spec.iron_control_principal = Some(principal_id.to_owned());
        upsert_spec_env(spec, "CENTAUR_MCP_PRINCIPAL_ID", principal_id.to_owned());
    }
    configure_tool_host_command(spec);
}

fn configure_tool_host_command(spec: &mut SandboxSpec) {
    if should_preserve_entrypoint_for_tool_host(spec) {
        spec.command = Some(vec!["/entrypoint.sh".to_owned()]);
        spec.args = vec!["centaur-tool-host".to_owned()];
    } else {
        spec.command = Some(vec!["centaur-tool-host".to_owned()]);
        spec.args.clear();
    }
}

fn should_preserve_entrypoint_for_tool_host(spec: &SandboxSpec) -> bool {
    spec.command
        .as_ref()
        .and_then(|command| command.first())
        .is_some_and(|program| program == "/entrypoint.sh")
        || spec.args.first().is_some_and(|arg| arg == "harness-server")
}

fn execution_metadata(
    metadata: Option<Value>,
    idle_timeout_ms: Option<u64>,
    max_duration_ms: Option<u64>,
) -> Value {
    let mut metadata = default_metadata(metadata);
    if let Value::Object(object) = &mut metadata {
        if let Some(value) = idle_timeout_ms {
            object.insert("idle_timeout_ms".to_owned(), json!(value));
        }
        if let Some(value) = max_duration_ms {
            object.insert("max_duration_ms".to_owned(), json!(value));
        }
    }
    metadata
}

fn merge_json_object(target: &mut Value, additions: Value) {
    let Some(target) = target.as_object_mut() else {
        return;
    };
    let Some(additions) = additions.as_object() else {
        return;
    };
    target.extend(additions.clone());
}

fn idle_timeout_from_execution(execution: &SessionExecution) -> Option<Duration> {
    execution
        .metadata
        .get("idle_timeout_ms")
        .and_then(Value::as_u64)
        .and_then(|value| nonzero_duration_millis(value).ok())
}

fn max_duration_from_execution(execution: &SessionExecution) -> Option<Duration> {
    execution
        .metadata
        .get("max_duration_ms")
        .and_then(Value::as_u64)
        .and_then(|value| nonzero_duration_millis(value).ok())
}

#[derive(Debug, Error)]
pub enum SessionRuntimeError {
    #[error("{0}")]
    BadRequest(String),
    #[error("control plane is shutting down")]
    ShuttingDown,
    #[error("timed out stopping stdout owner renewal for execution {execution_id}")]
    StdoutOwnerRenewerStopTimeout { execution_id: String },
    #[error("metadata trace configuration is no longer the active deployment generation")]
    InactiveMetadataTraceConfig,
    #[error("metadata trace consent changed before sandbox input")]
    MetadataTraceBoundaryChanged,
    #[error("sandbox assignment changed while replacing the previous sandbox")]
    SandboxAssignmentChanged,
    #[error(transparent)]
    Store(#[from] SessionStoreError),
    #[error(transparent)]
    Sandbox(#[from] SandboxError),
    #[error(transparent)]
    IronControl(#[from] centaur_iron_control::IronControlError),
    #[error(transparent)]
    WarmPool(#[from] WarmPoolError),
    #[error(
        "sandbox running capacity exceeded during {operation}: running={running}, max_running={max_running}"
    )]
    CapacityExceeded {
        max_running: usize,
        running: usize,
        operation: &'static str,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use centaur_sandbox_core::MountKind;
    use centaur_session_core::SessionStatus;
    use serde_json::json;
    use time::OffsetDateTime;

    #[test]
    fn sandbox_repo_cache_label_controls_access() {
        assert_eq!(
            sandbox_repo_cache_access_from_principal(&test_principal(
                std::collections::BTreeMap::new()
            )),
            SessionRepoCacheAccess::None
        );
        for value in ["none", "private", "bogus"] {
            assert_eq!(
                sandbox_repo_cache_access_from_principal(&test_principal(
                    std::collections::BTreeMap::from([(
                        SANDBOX_REPO_CACHE_LABEL.to_owned(),
                        value.to_owned(),
                    )])
                )),
                SessionRepoCacheAccess::None
            );
        }
        assert_eq!(
            sandbox_repo_cache_access_from_principal(&test_principal(
                std::collections::BTreeMap::from([(
                    SANDBOX_REPO_CACHE_LABEL.to_owned(),
                    "public".to_owned(),
                )])
            )),
            SessionRepoCacheAccess::Public
        );
        assert_eq!(
            sandbox_repo_cache_access_from_principal(&test_principal(
                std::collections::BTreeMap::from([(
                    SANDBOX_REPO_CACHE_LABEL.to_owned(),
                    "all".to_owned(),
                )])
            )),
            SessionRepoCacheAccess::All
        );
    }

    #[test]
    fn public_repo_cache_scopes_bind_mount_to_public_projection() {
        let mut spec = SandboxSpec::new("mock").mount(Mount::new(
            MountKind::Bind {
                source_path: "/var/lib/centaur/repos".to_owned(),
            },
            SANDBOX_REPOS_MOUNT_PATH,
        ));
        let capabilities = SessionSandboxCapabilities {
            repo_cache: SessionRepoCacheAccess::Public,
            observability_enabled: true,
            api_server_enabled: true,
            metadata_trace_enabled: false,
            metadata_trace_expires_at: None,
            metadata_trace_subject_hash: None,
            metadata_trace_consent_revision: None,
            metadata_trace_config_fingerprint: None,
            metadata_trace_config_generation: None,
        };

        apply_sandbox_capabilities(&mut spec, &capabilities);

        assert_eq!(spec.capabilities.repo_cache, RepoCacheAccess::Public);
        assert_eq!(
            env_value(&spec, "CENTAUR_SANDBOX_REPO_CACHE_ACCESS"),
            Some("public")
        );
        assert_eq!(
            spec.mounts[0].kind,
            MountKind::Bind {
                source_path: "/var/lib/centaur/repos/public".to_owned(),
            }
        );
        assert_eq!(spec.mounts[0].sub_path, None);
    }

    #[test]
    fn public_repo_cache_scopes_named_volume_to_public_subpath() {
        let mut spec = SandboxSpec::new("mock").mount(Mount::new(
            MountKind::NamedVolume("centaur-repo-cache".to_owned()),
            SANDBOX_REPOS_MOUNT_PATH,
        ));
        let capabilities = SessionSandboxCapabilities {
            repo_cache: SessionRepoCacheAccess::Public,
            observability_enabled: true,
            api_server_enabled: true,
            metadata_trace_enabled: false,
            metadata_trace_expires_at: None,
            metadata_trace_subject_hash: None,
            metadata_trace_consent_revision: None,
            metadata_trace_config_fingerprint: None,
            metadata_trace_config_generation: None,
        };

        apply_sandbox_capabilities(&mut spec, &capabilities);

        assert_eq!(
            spec.mounts[0].kind,
            MountKind::NamedVolume("centaur-repo-cache".to_owned())
        );
        assert_eq!(spec.mounts[0].sub_path.as_deref(), Some("public"));
    }

    #[test]
    fn public_repo_cache_scopes_skill_dirs_to_public_dirs() {
        let mut spec = SandboxSpec::new("mock")
            .env(
                CENTAUR_SKILL_DIRS_ENV,
                "/home/agent/github/acme/private/.agents/skills:\
                 /home/agent/github/acme/public/.agents/skills",
            )
            .env(
                CENTAUR_PUBLIC_SKILL_DIRS_ENV,
                "/home/agent/github/acme/public/.agents/skills",
            );
        let capabilities = SessionSandboxCapabilities {
            repo_cache: SessionRepoCacheAccess::Public,
            observability_enabled: true,
            api_server_enabled: true,
            metadata_trace_enabled: false,
            metadata_trace_expires_at: None,
            metadata_trace_subject_hash: None,
            metadata_trace_consent_revision: None,
            metadata_trace_config_fingerprint: None,
            metadata_trace_config_generation: None,
        };

        apply_sandbox_capabilities(&mut spec, &capabilities);

        assert_eq!(
            env_value(&spec, CENTAUR_SKILL_DIRS_ENV),
            Some("/home/agent/github/acme/public/.agents/skills")
        );
        assert_eq!(env_value(&spec, CENTAUR_PUBLIC_SKILL_DIRS_ENV), None);
    }

    #[test]
    fn disabled_repo_cache_removes_repo_mount() {
        let mut spec = SandboxSpec::new("mock")
            .mount(Mount::new(
                MountKind::Bind {
                    source_path: "/var/lib/centaur/repos".to_owned(),
                },
                SANDBOX_REPOS_MOUNT_PATH,
            ))
            .mount(Mount::new(MountKind::EmptyDir, "/workspace"))
            .env(
                CENTAUR_SKILL_DIRS_ENV,
                "/home/agent/github/acme/private/.agents/skills",
            )
            .env(
                CENTAUR_PUBLIC_SKILL_DIRS_ENV,
                "/home/agent/github/acme/public/.agents/skills",
            );
        let capabilities = SessionSandboxCapabilities {
            repo_cache: SessionRepoCacheAccess::None,
            observability_enabled: true,
            api_server_enabled: true,
            metadata_trace_enabled: false,
            metadata_trace_expires_at: None,
            metadata_trace_subject_hash: None,
            metadata_trace_consent_revision: None,
            metadata_trace_config_fingerprint: None,
            metadata_trace_config_generation: None,
        };

        apply_sandbox_capabilities(&mut spec, &capabilities);

        assert_eq!(spec.capabilities.repo_cache, RepoCacheAccess::None);
        assert_eq!(spec.mounts.len(), 1);
        assert_eq!(spec.mounts[0].target_path, "/workspace");
        assert_eq!(env_value(&spec, CENTAUR_SKILL_DIRS_ENV), None);
        assert_eq!(env_value(&spec, CENTAUR_PUBLIC_SKILL_DIRS_ENV), None);
    }

    #[test]
    fn non_codex_sandbox_disables_metadata_trace_and_scrubs_otlp_exporters() {
        let mut spec = SandboxSpec::new("mock")
            .label("centaur.ai/harness", "claude-code")
            .env("OTEL_EXPORTER_OTLP_ENDPOINT", "https://unreviewed.example")
            .env("OTEL_TRACES_EXPORTER", "otlp")
            .env("OTEL_METRICS_EXPORTER", "otlp")
            .env("OTEL_PROPAGATORS", "tracecontext")
            .env("OTEL_RESOURCE_ATTRIBUTES", "actor.id=U-sensitive");
        let capabilities = SessionSandboxCapabilities {
            metadata_trace_enabled: true,
            metadata_trace_expires_at: Some(OffsetDateTime::now_utc() + TimeDuration::hours(1)),
            metadata_trace_subject_hash: Some("subject".to_owned()),
            metadata_trace_consent_revision: Some(1),
            metadata_trace_config_fingerprint: Some("config".to_owned()),
            metadata_trace_config_generation: Some(1),
            ..SessionSandboxCapabilities::default_enabled()
        };

        apply_sandbox_capabilities(&mut spec, &capabilities);

        assert!(!spec.capabilities.metadata_trace_enabled);
        assert!(spec.env.iter().all(|env| !env.name.starts_with("OTEL_")));
        assert_eq!(
            env_value(&spec, "CENTAUR_METADATA_TRACE_CONSENT_EXPIRES_AT_UNIX"),
            None
        );
    }

    fn test_principal(
        labels: std::collections::BTreeMap<String, String>,
    ) -> centaur_iron_control::Principal {
        centaur_iron_control::Principal {
            id: "prn_test".to_owned(),
            namespace: "default".to_owned(),
            foreign_id: Some("slack-channel-t-c".to_owned()),
            name: "Test".to_owned(),
            labels,
            sandbox_observability_enabled: true,
            sandbox_api_server_enabled: true,
        }
    }

    #[test]
    fn actor_trace_revoke_and_config_change_replace_the_sandbox() {
        let principal = test_principal(BTreeMap::new());
        let config_a = MetadataTraceConfigIdentity {
            generation: 1,
            fingerprint: "config-a".to_owned(),
            enabled: true,
        };
        let config_b = MetadataTraceConfigIdentity {
            generation: 2,
            fingerprint: "config-b".to_owned(),
            enabled: true,
        };
        let mut consent = MetadataTraceConsent {
            source: "slack".to_owned(),
            workspace_id: "T1".to_owned(),
            user_id: "U1".to_owned(),
            enabled: true,
            expires_at: Some(OffsetDateTime::now_utc() + TimeDuration::hours(1)),
            revision: 1,
            drain_pending: false,
        };
        let subject = SlackTraceSubject::from_execution_metadata(
            "slack:T1:C1:1.2",
            Some(&json!({ "slack_actor_team_id": "T1", "slack_actor_user_id": "U1" })),
        )
        .unwrap();
        let active = sandbox_capabilities_with_trace_subject(
            sandbox_capabilities_from_principal(&principal),
            &subject,
            &consent,
            Some(&config_a),
        );
        assert!(active.metadata_trace_enabled);
        assert_eq!(
            active.metadata_trace_config_fingerprint.as_deref(),
            Some("config-a")
        );

        let revoked = sandbox_capabilities_from_principal(&test_principal(BTreeMap::new()));
        assert!(!sandbox_capabilities_match(Some(&active), &revoked));

        let changed = sandbox_capabilities_with_trace_subject(
            sandbox_capabilities_from_principal(&principal),
            &subject,
            &consent,
            Some(&config_b),
        );
        assert!(!sandbox_capabilities_match(Some(&active), &changed));

        let other_subject = SlackTraceSubject::from_execution_metadata(
            "slack:T1:C1:1.2",
            Some(&json!({ "slack_actor_team_id": "T1", "slack_actor_user_id": "U2" })),
        )
        .unwrap();
        let other_actor = sandbox_capabilities_with_trace_subject(
            sandbox_capabilities_from_principal(&principal),
            &other_subject,
            &consent,
            Some(&config_a),
        );
        assert!(!sandbox_capabilities_match(Some(&active), &other_actor));

        let disabled = MetadataTraceConfigIdentity {
            generation: 3,
            fingerprint: "disabled".to_owned(),
            enabled: false,
        };
        let disabled_capabilities = sandbox_capabilities_with_trace_subject(
            sandbox_capabilities_from_principal(&principal),
            &subject,
            &consent,
            Some(&disabled),
        );
        assert!(!disabled_capabilities.metadata_trace_enabled);
        assert!(!sandbox_capabilities_match(
            Some(&active),
            &disabled_capabilities
        ));

        consent.expires_at = Some(OffsetDateTime::now_utc() - TimeDuration::seconds(1));
        assert!(
            !sandbox_capabilities_with_trace_subject(
                sandbox_capabilities_from_principal(&principal),
                &subject,
                &consent,
                Some(&config_a)
            )
            .metadata_trace_enabled
        );

        consent.expires_at = Some(OffsetDateTime::now_utc() + TimeDuration::hours(25));
        assert!(
            !sandbox_capabilities_with_trace_subject(
                sandbox_capabilities_from_principal(&principal),
                &subject,
                &consent,
                Some(&config_a)
            )
            .metadata_trace_enabled
        );
    }

    #[test]
    fn trace_subject_matching_rejects_missing_and_mixed_actor_boundaries() {
        let thread_key = ThreadKey::parse("slack:T1:C1:1.2").unwrap();
        let missing_hash = session_execution(
            "exec-missing-subject",
            ExecutionStatus::Running,
            json!({ "metadata_trace_enabled": true }),
        );
        let u1 = SessionMessageInput {
            client_message_id: Some("u1".to_owned()),
            role: MessageRole::User,
            parts: vec![json!({ "type": "text", "text": "u1" })],
            metadata: json!({ "slack_actor_team_id": "T1", "slack_actor_user_id": "U1" }),
        };
        let u2 = SessionMessageInput {
            client_message_id: Some("u2".to_owned()),
            role: MessageRole::User,
            parts: vec![json!({ "type": "text", "text": "u2" })],
            metadata: json!({ "slack_actor_team_id": "T1", "slack_actor_user_id": "U2" }),
        };
        let actorless = SessionMessageInput {
            client_message_id: Some("actorless".to_owned()),
            role: MessageRole::User,
            parts: vec![json!({ "type": "text", "text": "actorless" })],
            metadata: json!({}),
        };
        assert!(!messages_match_active_trace_subject(
            &thread_key,
            Some(&missing_hash),
            std::slice::from_ref(&u1),
        ));

        let u1_hash = trace_subject_hash(
            &SlackTraceSubject::from_execution_metadata(thread_key.as_str(), Some(&u1.metadata))
                .unwrap(),
        );
        let active = session_execution(
            "exec-u1",
            ExecutionStatus::Running,
            json!({ "metadata_trace_enabled": true, "metadata_trace_subject_hash": u1_hash }),
        );
        assert!(!messages_match_active_trace_subject(
            &thread_key,
            Some(&active),
            &[u1, u2]
        ));
        assert!(!messages_match_active_trace_subject(
            &thread_key,
            Some(&active),
            std::slice::from_ref(&actorless),
        ));
    }

    #[test]
    fn persona_registry_validates_default_and_summarizes_without_prompt() {
        let registry = PersonaRegistry::new(
            [PersonaDefinition {
                id: "eng".to_owned(),
                source_root: "/repo/tools".to_owned(),
                source_path: "/repo/tools/personas/eng".to_owned(),
                source_ref: Some("abc123".to_owned()),
                prompt_hash: "sha256:prompt".to_owned(),
                prompt: "secret prompt".to_owned(),
            }],
            Some("eng".to_owned()),
            vec!["/repo/tools".to_owned()],
        )
        .unwrap();

        let summaries = registry.summaries();

        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].id, "eng");
        assert!(
            serde_json::to_value(registry.get("eng").unwrap())
                .unwrap()
                .get("prompt")
                .is_none()
        );
        assert!(PersonaRegistry::new(Vec::new(), Some("missing".to_owned()), Vec::new()).is_err());
    }

    #[test]
    fn persona_registry_limits_public_access_to_public_source_roots() {
        let registry = PersonaRegistry::new(
            [
                PersonaDefinition {
                    id: "private".to_owned(),
                    source_root: "/repo/private/tools".to_owned(),
                    source_path: "/repo/private/tools/personas/private".to_owned(),
                    source_ref: None,
                    prompt_hash: "sha256:private".to_owned(),
                    prompt: "private prompt".to_owned(),
                },
                PersonaDefinition {
                    id: "public".to_owned(),
                    source_root: "/repo/public/tools".to_owned(),
                    source_path: "/repo/public/tools/personas/public".to_owned(),
                    source_ref: None,
                    prompt_hash: "sha256:public".to_owned(),
                    prompt: "public prompt".to_owned(),
                },
            ],
            Some("private".to_owned()),
            vec![
                "/repo/private/tools".to_owned(),
                "/repo/public/tools".to_owned(),
            ],
        )
        .unwrap()
        .with_public_source_roots(["/repo/public/tools".to_owned()]);

        assert_eq!(
            registry.default_persona_id_for_access(&SessionRepoCacheAccess::All),
            Some("private")
        );
        assert_eq!(
            registry.default_persona_id_for_access(&SessionRepoCacheAccess::Public),
            None
        );
        assert!(
            registry
                .context_for_access("private", false, &SessionRepoCacheAccess::Public)
                .is_err()
        );
        assert_eq!(
            registry
                .context_for_access("public", false, &SessionRepoCacheAccess::Public)
                .unwrap()
                .persona_id,
            "public"
        );
    }

    #[test]
    fn tool_host_command_preserves_sandbox_entrypoint_for_tool_setup() {
        let thread_key = ThreadKey::parse("mcp:test").unwrap();
        let workload = SandboxWorkloadMode::codex_app_server(
            "centaur-agent:latest",
            [("TOOL_DIRS".to_owned(), "/app/tools".to_owned())],
            HarnessType::Codex,
        );
        let mut spec = workload.spec(&thread_key, &HarnessType::Codex, None);

        configure_tool_host_command(&mut spec);

        assert_eq!(spec.command, Some(vec!["/entrypoint.sh".to_owned()]));
        assert_eq!(spec.args, vec!["centaur-tool-host"]);
        assert_eq!(env_value(&spec, "TOOL_DIRS"), Some("/app/tools"));
    }

    #[test]
    fn turn_completed_without_answer_text_is_terminal() {
        let event = json!({
            "type": "turn.completed",
            "turn": {"id": "turn-1", "status": "completed"},
        });

        assert_eq!(
            terminal_output(&event, ""),
            Some(TerminalOutput::Completed {
                reason: "turn_completed",
                result_text: None
            })
        );
    }

    #[test]
    fn turn_completed_after_answer_text_is_terminal() {
        let delta = json!({
            "method": "item/agentMessage/delta",
            "params": {"turnId": "turn-1", "delta": "Final answer"},
        });
        let terminal = json!({
            "method": "turn/completed",
            "params": {"turn": {"id": "turn-1", "status": "completed"}},
        });

        assert!(matches!(
            output_line_final_answer_text(&delta),
            Some(FinalAnswerTextUpdate::Append(_))
        ));
        assert_eq!(
            terminal_output(&terminal, "Final answer"),
            Some(TerminalOutput::Completed {
                reason: "turn_completed",
                result_text: Some("Final answer".to_owned())
            })
        );
    }

    #[test]
    fn nanocodex_native_events_supply_answer_and_terminal_output() {
        let delta = json!({
            "protocol_version": 1,
            "request_id": "nano-1",
            "seq": 2,
            "type": "assistant.delta",
            "payload": {"text": "Final answer"},
        });
        let terminal = json!({
            "protocol_version": 1,
            "request_id": "nano-1",
            "seq": 3,
            "type": "run.completed",
            "payload": {"status": "completed"},
        });

        let Some(FinalAnswerTextUpdate::Append(answer)) = output_line_final_answer_text(&delta)
        else {
            panic!("Nanocodex delta should append final-answer text")
        };
        assert_eq!(
            terminal_output(&terminal, &answer),
            Some(TerminalOutput::Completed {
                reason: "run_completed",
                result_text: Some("Final answer".to_owned())
            })
        );
    }

    #[test]
    fn nanocodex_commentary_is_not_terminal_answer_text() {
        for event_type in ["assistant.delta", "assistant.message"] {
            let commentary = json!({
                "protocol_version": 1,
                "request_id": "nano-1",
                "seq": 2,
                "type": event_type,
                "payload": {
                    "item_id": "commentary-1",
                    "phase": "commentary",
                    "text": "I’ll verify."
                },
            });
            assert!(output_line_final_answer_text(&commentary).is_none());
        }

        let final_answer = json!({
            "protocol_version": 1,
            "request_id": "nano-1",
            "seq": 3,
            "type": "assistant.message",
            "payload": {
                "item_id": "answer-1",
                "phase": "final_answer",
                "text": "Done."
            },
        });
        let Some(FinalAnswerTextUpdate::Replace(text)) =
            output_line_final_answer_text(&final_answer)
        else {
            panic!("final Nanocodex message should replace terminal answer text")
        };
        assert_eq!(text, "Done.");
    }

    #[test]
    fn nanocodex_run_error_waits_for_run_failed() {
        let event = json!({
            "protocol_version": 1,
            "request_id": "nano-1",
            "seq": 2,
            "type": "run.error",
            "payload": {"message": "proxy refused"},
        });
        assert_eq!(terminal_output(&event, ""), None);

        let terminal = json!({
            "protocol_version": 1,
            "request_id": "nano-1",
            "seq": 3,
            "type": "run.failed",
            "payload": {"status": "failed"},
        });
        assert_eq!(
            terminal_output(&terminal, ""),
            Some(TerminalOutput::Failed {
                error: "terminal harness output reported failure".to_owned()
            })
        );
    }

    #[test]
    fn nanocodex_cancelled_run_uses_the_existing_cancellation_path() {
        let terminal = json!({
            "protocol_version": 1,
            "request_id": "nano-1",
            "seq": 2,
            "type": "run.failed",
            "payload": {"status": "cancelled"},
        });
        assert_eq!(
            terminal_output(&terminal, ""),
            Some(TerminalOutput::Cancelled {
                reason: "turn_interrupted"
            })
        );
    }

    #[test]
    fn turn_completed_uses_completed_agent_message_text_when_terminal_is_empty() {
        let completed = json!({
            "type": "item.completed",
            "item": {
                "id": "msg-final",
                "type": "agentMessage",
                "phase": "final_answer",
                "text": "1. No new findings.\n\n2. No writes were used."
            }
        });
        let terminal = json!({
            "type": "turn.completed",
            "turn": {"id": "turn-1", "status": "completed"},
        });

        let Some(FinalAnswerTextUpdate::Replace(final_text)) =
            output_line_final_answer_text(&completed)
        else {
            panic!("completed agentMessage should replace final answer text")
        };
        assert_eq!(
            terminal_output(&terminal, &final_text),
            Some(TerminalOutput::Completed {
                reason: "turn_completed",
                result_text: Some("1. No new findings.\n\n2. No writes were used.".to_owned())
            })
        );
    }

    #[test]
    fn interrupted_turn_completed_without_answer_is_cancelled() {
        let event = json!({
            "type": "turn.completed",
            "turn": {"id": "turn-1", "status": "interrupted"},
        });

        assert_eq!(
            terminal_output(&event, ""),
            Some(TerminalOutput::Cancelled {
                reason: "turn_interrupted"
            })
        );
    }

    #[test]
    fn interrupted_turn_completed_after_answer_stays_terminal() {
        let event = json!({
            "method": "turn/completed",
            "params": {"turn": {"id": "turn-1", "status": "interrupted"}},
        });

        assert_eq!(
            terminal_output(&event, "Final answer"),
            Some(TerminalOutput::Completed {
                reason: "turn_completed",
                result_text: Some("Final answer".to_owned())
            })
        );
    }

    #[test]
    fn terminal_result_completes_even_without_prior_delta() {
        let event = json!({
            "type": "result",
            "result": {"text": "Final answer"},
        });

        assert_eq!(
            terminal_output(&event, ""),
            Some(TerminalOutput::Completed {
                reason: "result",
                result_text: Some("Final answer".to_owned())
            })
        );
    }

    #[test]
    fn turn_done_carries_terminal_result_text() {
        let event = json!({
            "type": "turn.done",
            "result": "Final answer",
        });

        assert_eq!(
            terminal_output(&event, ""),
            Some(TerminalOutput::Completed {
                reason: "turn_done",
                result_text: Some("Final answer".to_owned())
            })
        );
    }

    #[test]
    fn turn_failed_is_terminal_failure() {
        let event = json!({
            "type": "turn.failed",
            "error": "sandbox exited",
        });

        assert_eq!(
            terminal_output(&event, ""),
            Some(TerminalOutput::Failed {
                error: "sandbox exited".to_owned()
            })
        );
    }

    #[test]
    fn nested_terminal_text_is_normalized() {
        let event = json!({
            "result": {
                "message": {
                    "content": [{"type": "text", "text": "Final answer"}],
                },
            },
        });

        assert_eq!(terminal_payload_text(&event), "Final answer");
    }

    #[test]
    fn timeout_event_uses_millisecond_duration() {
        assert_eq!(duration_millis_u64(Duration::from_millis(3_000)), 3_000);
    }

    #[test]
    fn stdout_state_first_token_detection_uses_answer_text() {
        let state = StdoutPumpState::default();
        let turn_started = json!({"type": "turn.started", "turn_id": "turn-1"});
        let delta = json!({
            "type": "item.agentMessage.delta",
            "turnId": "turn-1",
            "itemId": "msg-1",
            "delta": "Hello"
        });
        let terminal_result = json!({"type": "result", "result": {"text": "Done"}});

        assert!(!state.should_record_first_token("exe-1", Some(&turn_started)));
        assert!(state.should_record_first_token("exe-1", Some(&delta)));
        assert!(state.should_record_first_token("exe-2", Some(&terminal_result)));
    }

    #[test]
    fn terminal_failure_class_is_low_cardinality() {
        assert_eq!(
            terminal_failure_class("sandbox stdout closed before terminal output"),
            "sandbox_io"
        );
        assert_eq!(
            terminal_failure_class("execution orphaned by control plane restart"),
            "orphaned"
        );
        assert_eq!(
            terminal_failure_class("turn failed: model error"),
            "harness"
        );
    }

    #[test]
    fn execution_metadata_preserves_idle_and_max_duration() {
        let metadata =
            execution_metadata(Some(json!({"source": "test"})), Some(2_000), Some(5_000));

        assert_eq!(metadata["source"], "test");
        assert_eq!(metadata["idle_timeout_ms"], 2_000);
        assert_eq!(metadata["max_duration_ms"], 5_000);
    }

    #[test]
    fn idle_timeout_is_read_from_execution_metadata() {
        let execution = session_execution(
            "exe-idle",
            ExecutionStatus::Completed,
            json!({"idle_timeout_ms": 1500}),
        );

        assert_eq!(
            idle_timeout_from_execution(&execution),
            Some(Duration::from_millis(1500))
        );
    }

    #[test]
    fn redacts_sensitive_values_from_output_lines() {
        let line = r#"{"type":"item.completed","item":{"aggregatedOutput":"Authorization: Bearer sbx1.threadpayload.signature\nSANDBOX_TOKEN=sbx1.otherpayload.othersig\nSLACK_BOT_TOKEN=xoxb-1234567890-abcdef\n"}}"#;

        let redacted = redact_sensitive_text(line);

        assert!(!redacted.contains("sbx1.threadpayload.signature"));
        assert!(!redacted.contains("sbx1.otherpayload.othersig"));
        assert!(!redacted.contains("xoxb-1234567890-abcdef"));
        assert!(redacted.contains("Authorization: Bearer [REDACTED_TOKEN]"));
        assert!(redacted.contains("SANDBOX_TOKEN=[REDACTED_TOKEN]"));
        assert!(redacted.contains("SLACK_BOT_TOKEN=[REDACTED_TOKEN]"));
    }

    #[test]
    fn prefixed_token_redaction_preserves_ordinary_hyphenated_words() {
        let line = "risk-adjusted PnL improved while sk-proj-abcdefghijklmnopqrstuvwxyz123456 stayed hidden";

        let redacted = redact_sensitive_text(line);

        assert!(redacted.contains("risk-adjusted PnL improved"));
        assert!(!redacted.contains("sk-proj-abcdefghijklmnopqrstuvwxyz123456"));
        assert!(redacted.contains("[REDACTED_TOKEN] stayed hidden"));
    }

    #[test]
    fn codex_app_server_event_source_and_type_are_classified() {
        let app_server = json!({
            "method": "item/agentMessage/delta",
            "params": {"turnId": "turn-1", "itemId": "item-1"},
        });
        let harness = json!({
            "type": "assistant",
            "message": {"content": [{"type": "text", "text": "redacted"}]},
        });
        let sandbox = json!({
            "type": "custom.wrapper.event",
        });

        assert_eq!(
            sandbox_output_event_type(&app_server),
            "item/agentMessage/delta"
        );
        assert_eq!(sandbox_output_source(&app_server), "codex_app_server");
        assert_eq!(sandbox_output_source(&harness), "harness");
        assert_eq!(sandbox_output_source(&sandbox), "sandbox");
    }

    #[test]
    fn codex_app_server_mcp_tool_events_emit_tool_spans() {
        let started = json!({
            "method": "item/started",
            "params": {
                "item": {
                    "id": "tool-1",
                    "type": "mcpToolCall",
                    "server": "github",
                    "tool": "list_issues"
                }
            }
        });
        let progress = json!({
            "method": "item/mcpToolCall/progress",
            "params": {"itemId": "tool-1"}
        });
        let completed = json!({
            "method": "item/completed",
            "params": {
                "item": {
                    "id": "tool-1",
                    "durationMs": 125
                }
            }
        });
        let mut known = HashMap::new();

        assert_eq!(
            tool_call_span_events(&started, &mut known),
            vec![ToolCallSpanEvent {
                labels: ToolCallLabels {
                    kind: "mcp".to_owned(),
                    name: "list_issues".to_owned(),
                    method: "github".to_owned(),
                },
                status: "started",
                duration: None,
            }]
        );
        assert_eq!(
            tool_call_span_events(&progress, &mut known),
            vec![ToolCallSpanEvent {
                labels: ToolCallLabels {
                    kind: "mcp".to_owned(),
                    name: "list_issues".to_owned(),
                    method: "github".to_owned(),
                },
                status: "progress",
                duration: None,
            }]
        );
        assert_eq!(
            tool_call_span_events(&completed, &mut known),
            vec![ToolCallSpanEvent {
                labels: ToolCallLabels {
                    kind: "mcp".to_owned(),
                    name: "list_issues".to_owned(),
                    method: "github".to_owned(),
                },
                status: "completed",
                duration: Some(Duration::from_millis(125)),
            }]
        );
        assert!(known.is_empty());
    }

    #[test]
    fn command_execution_items_do_not_emit_tool_spans() {
        let started = json!({
            "method": "item/started",
            "params": {
                "item": {
                    "id": "cmd-1",
                    "type": "commandExecution",
                    "command": "ls -la"
                }
            }
        });
        let completed = json!({
            "method": "item/completed",
            "params": {
                "item": {
                    "id": "cmd-1",
                    "type": "commandExecution",
                    "command": "ls -la",
                    "exitCode": 0,
                    "durationMs": 42
                }
            }
        });
        let mut known = HashMap::new();

        assert_eq!(tool_call_span_events(&started, &mut known), Vec::new());
        assert_eq!(tool_call_span_events(&completed, &mut known), Vec::new());
        assert!(known.is_empty());
    }

    #[test]
    fn anthropic_tool_use_and_result_events_emit_tool_spans() {
        let assistant = json!({
            "type": "assistant",
            "message": {
                "content": [
                    {"type": "tool_use", "id": "use-1", "name": "todo_write", "input": {"redacted": true}}
                ]
            }
        });
        let result = json!({
            "type": "user",
            "content": [
                {"type": "tool_result", "tool_use_id": "use-1", "content": "redacted"}
            ]
        });
        let mut known = HashMap::new();

        assert_eq!(
            tool_call_span_events(&assistant, &mut known),
            vec![ToolCallSpanEvent {
                labels: ToolCallLabels {
                    kind: "anthropic".to_owned(),
                    name: "todo_write".to_owned(),
                    method: "call".to_owned(),
                },
                status: "started",
                duration: None,
            }]
        );
        assert_eq!(
            tool_call_span_events(&result, &mut known),
            vec![ToolCallSpanEvent {
                labels: ToolCallLabels {
                    kind: "anthropic".to_owned(),
                    name: "todo_write".to_owned(),
                    method: "call".to_owned(),
                },
                status: "completed",
                duration: None,
            }]
        );
        assert!(known.is_empty());
    }

    #[test]
    fn idle_pause_requires_latest_terminal_execution_and_same_sandbox() {
        let session = session_with_sandbox("asbx-1");
        let completed = session_execution("exe-1", ExecutionStatus::Completed, json!({}));
        let running = session_execution("exe-1", ExecutionStatus::Running, json!({}));
        let newer = session_execution("exe-2", ExecutionStatus::Completed, json!({}));

        assert!(should_pause_idle_sandbox(
            &session,
            Some(&completed),
            "exe-1",
            "asbx-1"
        ));
        assert!(!should_pause_idle_sandbox(
            &session,
            Some(&running),
            "exe-1",
            "asbx-1"
        ));
        assert!(!should_pause_idle_sandbox(
            &session,
            Some(&newer),
            "exe-1",
            "asbx-1"
        ));
        assert!(!should_pause_idle_sandbox(
            &session,
            Some(&completed),
            "exe-1",
            "asbx-other"
        ));
    }

    #[test]
    fn event_stream_attaches_only_to_running_sandboxes() {
        assert!(should_attach_session_pipe(&SandboxStatus::Running));
        assert!(!should_attach_session_pipe(&SandboxStatus::Created));
        assert!(!should_attach_session_pipe(&SandboxStatus::Suspended));
        assert!(!should_attach_session_pipe(&SandboxStatus::Stopped));
        assert!(!should_attach_session_pipe(&SandboxStatus::Gone));
        assert!(!should_attach_session_pipe(&SandboxStatus::Unknown(
            "other".to_owned()
        )));
    }

    #[test]
    fn existing_sandbox_action_repairs_or_replaces_non_attachable_assignments() {
        assert_eq!(
            existing_sandbox_action(&SandboxStatus::Running),
            ExistingSandboxAction::Reuse
        );
        assert_eq!(
            existing_sandbox_action(&SandboxStatus::Suspended),
            ExistingSandboxAction::ResumeOrReplace
        );
        assert_eq!(
            existing_sandbox_action(&SandboxStatus::Created),
            ExistingSandboxAction::ResumeOrReplace
        );
        assert_eq!(
            existing_sandbox_action(&SandboxStatus::Stopped),
            ExistingSandboxAction::Replace
        );
        assert_eq!(
            existing_sandbox_action(&SandboxStatus::Gone),
            ExistingSandboxAction::Replace
        );
        assert_eq!(
            existing_sandbox_action(&SandboxStatus::Unknown("rollout missing".to_owned())),
            ExistingSandboxAction::Replace
        );
    }

    #[test]
    fn event_stream_tolerates_not_ready_attach_race() {
        let not_ready =
            SessionRuntimeError::Sandbox(SandboxError::NotReady("sandbox paused".to_owned()));
        let backend_error = SessionRuntimeError::Sandbox(SandboxError::backend("api failed"));

        assert!(is_event_stream_attach_race(&not_ready));
        assert!(!is_event_stream_attach_race(&backend_error));
    }

    #[test]
    fn stdout_state_drops_late_output_from_inactive_turn() {
        let mut state = StdoutPumpState::default();
        let started = r#"{"type":"turn.started","turn_id":"turn-old"}"#;
        let delta = r#"{"type":"item.agentMessage.delta","turnId":"turn-old","itemId":"msg-old","delta":"late"}"#;

        assert_eq!(
            state.execution_for_line(Some("exe-old"), started),
            Some("exe-old".to_owned())
        );
        assert_eq!(state.execution_for_line(None, delta), None);
        assert_eq!(state.execution_for_line(Some("exe-new"), delta), None);
    }

    #[test]
    fn stdout_state_uses_final_agent_message_when_turn_completed_is_textless() {
        let mut state = StdoutPumpState::default();
        let started = r#"{"type":"turn.started","turn_id":"turn-1"}"#;
        let delta = r#"{"type":"item.agentMessage.delta","turnId":"turn-1","itemId":"msg-final","delta":"draft"}"#;
        let completed = r#"{"type":"item.completed","item":{"id":"msg-final","type":"agentMessage","phase":"final_answer","text":"Final canonical answer."}}"#;
        let terminal =
            r#"{"type":"turn.completed","turn":{"id":"turn-1","status":"completed"},"usage":null}"#;

        assert_eq!(
            state.execution_for_line(Some("exe-1"), started),
            Some("exe-1".to_owned())
        );
        assert_eq!(state.observe("exe-1", delta), None);
        assert_eq!(state.observe("exe-1", completed), None);
        assert_eq!(
            state.observe("exe-1", terminal),
            Some(TerminalOutput::Completed {
                reason: "turn_completed",
                result_text: Some("Final canonical answer.".to_owned())
            })
        );
    }

    #[test]
    fn stdout_state_replay_waits_for_root_turn_after_subagent_completion() {
        let lines = [
            r#"{"method":"thread/started","params":{"thread":{"id":"root-thread"}}}"#,
            r#"{"method":"turn/started","params":{"threadId":"root-thread","turn":{"id":"root-turn"}}}"#,
            r#"{"method":"thread/started","params":{"thread":{"id":"child-thread"}}}"#,
            r#"{"method":"turn/started","params":{"threadId":"child-thread","turn":{"id":"child-turn"}}}"#,
            r#"{"method":"item/completed","params":{"threadId":"child-thread","turnId":"child-turn","item":{"id":"child-answer","type":"agentMessage","phase":"final_answer","text":"Child answer."}}}"#,
            r#"{"method":"turn/completed","params":{"threadId":"child-thread","turn":{"id":"child-turn","status":"completed"}}}"#,
            r#"{"method":"item/completed","params":{"threadId":"root-thread","turnId":"root-turn","item":{"id":"root-answer","type":"agentMessage","phase":"final_answer","text":"Root answer."}}}"#,
            r#"{"method":"turn/completed","params":{"threadId":"root-thread","turn":{"id":"root-turn","status":"completed"}}}"#,
        ];
        let mut state = StdoutPumpState::default();
        let recorded = lines[..2]
            .iter()
            .map(|line| (*line).to_owned())
            .collect::<Vec<_>>();

        assert_eq!(state.replay_recorded_output("exe-1", &recorded), None);
        assert_eq!(state.root_thread_id("exe-1"), Some("root-thread"));

        for line in &lines[2..6] {
            assert_eq!(
                state.execution_for_line(Some("exe-1"), line),
                Some("exe-1".to_owned())
            );
            assert_eq!(state.observe("exe-1", line), None);
        }
        assert_eq!(
            state.root_thread_id("exe-1"),
            Some("root-thread"),
            "a child thread start must not replace the replayed root"
        );
        let child_only = lines[..6]
            .iter()
            .map(|line| (*line).to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            StdoutPumpState::default().replay_recorded_output("recorded-output", &child_only),
            None
        );

        assert_eq!(
            state.execution_for_line(Some("exe-1"), lines[6]),
            Some("exe-1".to_owned())
        );
        assert_eq!(state.observe("exe-1", lines[6]), None);
        assert_eq!(
            state.execution_for_line(Some("exe-1"), lines[7]),
            Some("exe-1".to_owned())
        );
        let expected = Some(TerminalOutput::Completed {
            reason: "turn_completed",
            result_text: Some("Root answer.".to_owned()),
        });
        assert_eq!(state.observe("exe-1", lines[7]), expected);
        assert_eq!(
            StdoutPumpState::default()
                .replay_recorded_output("recorded-output", &lines.map(str::to_owned)),
            expected
        );
    }

    #[test]
    fn stdout_state_preserves_authoritative_durable_root_for_child_only_history() {
        let root_started =
            r#"{"method":"thread/started","params":{"thread":{"id":"recorded-root"}}}"#;
        let mut replayed = StdoutPumpState::default();
        replayed.seed_root_thread_id_if_absent("exe-replayed", "stale-durable-child");
        assert_eq!(
            replayed.replay_recorded_output("exe-replayed", &[root_started.to_owned()]),
            None
        );
        assert_eq!(
            replayed.root_thread_id("exe-replayed"),
            Some("recorded-root"),
            "recorded first-root identity must win over the durable fallback"
        );

        let child_completed = r#"{"method":"turn/completed","params":{"threadId":"child-thread","turn":{"id":"child-turn","status":"completed"}}}"#;
        let root_completed = r#"{"method":"turn/completed","params":{"threadId":"durable-root","turn":{"id":"root-turn","status":"completed"}}}"#;
        let mut fallback = StdoutPumpState::default();
        fallback.set_authoritative_root_thread_id("exe-fallback", "durable-root");
        assert_eq!(
            fallback.replay_recorded_output(
                "exe-fallback",
                &[
                    r#"{"method":"thread/started","params":{"thread":{"id":"child-thread"}}}"#
                        .to_owned(),
                    child_completed.to_owned(),
                ],
            ),
            None
        );
        assert_eq!(
            fallback.root_thread_id("exe-fallback"),
            Some("durable-root"),
            "child-only recorded output must not replace the durable root"
        );
        assert_eq!(
            fallback.replay_output_lines("exe-fallback", &[root_completed.to_owned()]),
            Some(TerminalOutput::Completed {
                reason: "turn_completed",
                result_text: None,
            })
        );
    }

    #[test]
    fn stdout_state_keeps_nanocodex_runs_separate_by_request_id() {
        let mut state = StdoutPumpState::default();
        let first_started =
            r#"{"protocol_version":1,"request_id":"nano-1","seq":1,"type":"run.started"}"#;
        let first_terminal = r#"{"protocol_version":1,"request_id":"nano-1","seq":2,"type":"run.completed","payload":{"status":"completed"}}"#;
        let second_started =
            r#"{"protocol_version":1,"request_id":"nano-2","seq":1,"type":"run.started"}"#;
        let second_answer = r#"{"protocol_version":1,"request_id":"nano-2","seq":2,"type":"assistant.delta","payload":{"text":"Second answer."}}"#;
        let second_terminal = r#"{"protocol_version":1,"request_id":"nano-2","seq":3,"type":"run.completed","payload":{"status":"completed"}}"#;

        assert_eq!(
            state.execution_for_line(Some("exe-1"), first_started),
            Some("exe-1".to_owned())
        );
        assert_eq!(state.observe("exe-1", first_started), None);
        assert_eq!(
            state.execution_for_line(Some("exe-1"), first_terminal),
            Some("exe-1".to_owned())
        );
        assert!(state.observe("exe-1", first_terminal).is_some());
        state.forget("exe-1");

        assert_eq!(
            state.execution_for_line(Some("exe-2"), second_started),
            Some("exe-2".to_owned())
        );
        assert_eq!(state.observe("exe-2", second_started), None);
        assert_eq!(
            state.execution_for_line(Some("exe-2"), first_terminal),
            None,
            "a delayed terminal from the first request must not finish the second execution"
        );
        assert_eq!(
            state.execution_for_line(Some("exe-2"), second_answer),
            Some("exe-2".to_owned())
        );
        assert_eq!(state.observe("exe-2", second_answer), None);
        assert_eq!(
            state.execution_for_line(Some("exe-2"), second_terminal),
            Some("exe-2".to_owned())
        );
        assert_eq!(
            state.observe("exe-2", second_terminal),
            Some(TerminalOutput::Completed {
                reason: "run_completed",
                result_text: Some("Second answer.".to_owned()),
            })
        );
    }

    #[test]
    fn stdout_state_keeps_completed_codex_turn_and_item_ids_bounded() {
        let mut state = StdoutPumpState::default();
        state.set_authoritative_root_thread_id("exe-1", "root-thread");
        let first_started = r#"{"method":"turn/started","params":{"threadId":"root-thread","turn":{"id":"turn-old"}}}"#;
        let first_answer = r#"{"method":"item/completed","params":{"threadId":"root-thread","turnId":"turn-old","item":{"id":"item-old","type":"agentMessage","phase":"final_answer","text":"First."}}}"#;
        let first_terminal = r#"{"method":"turn/completed","params":{"threadId":"root-thread","turn":{"id":"turn-old","status":"completed"}}}"#;
        for line in [first_started, first_answer, first_terminal] {
            assert_eq!(
                state.execution_for_line(Some("exe-1"), line),
                Some("exe-1".to_owned())
            );
            state.observe("exe-1", line);
        }
        state.forget("exe-1");
        state.set_authoritative_root_thread_id("exe-2", "root-thread");
        assert_eq!(
            state.execution_for_line(Some("exe-2"), first_answer),
            None,
            "a late item from the prior turn must not attach to the new execution"
        );
        assert_eq!(
            state.execution_for_line(Some("exe-2"), first_terminal),
            None,
            "a late prior-turn terminal must not finish the new execution"
        );

        for index in 0..(COMPLETED_OUTPUT_ID_CAPACITY + 16) {
            let execution_id = format!("exe-bounded-{index}");
            let value = json!({
                "request_id": format!("request-{index}"),
                "turnId": format!("turn-{index}"),
                "itemId": format!("item-{index}"),
            });
            state.remember_value_execution(&value, &execution_id);
            state.forget(&execution_id);
        }
        assert!(state.completed_request_execution_by_id.len() <= COMPLETED_OUTPUT_ID_CAPACITY);
        assert!(state.completed_turn_execution_by_id.len() <= COMPLETED_OUTPUT_ID_CAPACITY);
        assert!(state.completed_item_execution_by_id.len() <= COMPLETED_OUTPUT_ID_CAPACITY);
        assert!(
            !state
                .completed_request_execution_by_id
                .contains_key("request-0")
        );
        assert!(
            state
                .completed_request_execution_by_id
                .contains_key(&format!("request-{}", COMPLETED_OUTPUT_ID_CAPACITY + 15))
        );
    }

    #[test]
    fn pipe_lock_registries_reuse_live_locks_and_reap_released_sandboxes() {
        let output_gates: SessionOutputGates = Arc::new(DashMap::new());
        let first = output_gate_from_registry(&output_gates, "sandbox-first");
        let first_again = output_gate_from_registry(&output_gates, "sandbox-first");
        assert!(
            Arc::ptr_eq(&first, &first_again),
            "concurrent users of one sandbox must share the same fence"
        );
        drop(first_again);
        drop(first);
        assert!(
            !output_gates.contains_key("sandbox-first"),
            "the final gate holder must remove its registry entry"
        );

        let second = output_gate_from_registry(&output_gates, "sandbox-second");
        assert_eq!(output_gates.len(), 1);
        assert!(output_gates.contains_key("sandbox-second"));
        let second_again = output_gate_from_registry(&output_gates, "sandbox-second");
        assert!(Arc::ptr_eq(&second, &second_again));

        let open_locks: SessionPipeOpenLocks = Arc::new(DashMap::new());
        let open_lock =
            registered_lock_from_registry(&open_locks, "sandbox-open", || Mutex::new(()));
        let open_lock_again =
            registered_lock_from_registry(&open_locks, "sandbox-open", || Mutex::new(()));
        assert!(Arc::ptr_eq(&open_lock, &open_lock_again));
        drop(open_lock_again);
        drop(open_lock);
        assert!(
            open_locks.is_empty(),
            "the final pipe-open lock holder must remove its registry entry"
        );

        for _ in 0..64 {
            let racing_gates: SessionOutputGates = Arc::new(DashMap::new());
            let retiring = output_gate_from_registry(&racing_gates, "sandbox-race");
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let drop_barrier = barrier.clone();
            let dropper = std::thread::spawn(move || {
                drop_barrier.wait();
                drop(retiring);
            });
            barrier.wait();
            let replacement = output_gate_from_registry(&racing_gates, "sandbox-race");
            dropper.join().expect("join retiring gate drop");
            let registered = racing_gates
                .get("sandbox-race")
                .and_then(|entry| entry.upgrade())
                .expect("replacement gate must remain registered");
            assert!(
                Arc::ptr_eq(&replacement, &registered),
                "retiring gate cleanup must not remove a concurrent replacement"
            );
        }
    }

    #[test]
    fn detached_replay_uses_live_answer_only_for_terminal_only_log_slice() {
        let answer = r#"{"method":"item/completed","params":{"threadId":"root-thread","turnId":"root-turn","item":{"id":"root-answer","type":"agentMessage","phase":"final_answer","text":"Root answer."}}}"#;
        let terminal = r#"{"method":"turn/completed","params":{"threadId":"root-thread","turn":{"id":"root-turn","status":"completed"}}}"#;
        let mut live = StdoutPumpState::default();
        live.set_authoritative_root_thread_id("exe-1", "root-thread");
        assert_eq!(live.observe("exe-1", answer), None);

        let expected = Some(TerminalOutput::Completed {
            reason: "turn_completed",
            result_text: Some("Root answer.".to_owned()),
        });
        assert_eq!(
            replay_detached_recorded_output(&live, "exe-1", &[terminal.to_owned()]),
            expected
        );
        assert_eq!(
            replay_detached_recorded_output(
                &live,
                "exe-1",
                &[answer.to_owned(), terminal.to_owned()],
            ),
            expected,
            "overlapping recorded output must not duplicate the live answer"
        );

        let mut live_prefix = StdoutPumpState::default();
        live_prefix.set_authoritative_root_thread_id("exe-2", "root-thread");
        let prefix = r#"{"type":"item.agentMessage.delta","threadId":"root-thread","turnId":"root-turn-2","itemId":"root-answer-2","delta":"Hello wor"}"#;
        let suffix = r#"{"type":"item.agentMessage.delta","threadId":"root-thread","turnId":"root-turn-2","itemId":"root-answer-2","delta":"ld."}"#;
        let suffix_terminal = r#"{"type":"turn.completed","threadId":"root-thread","turn":{"id":"root-turn-2","status":"completed"}}"#;
        assert_eq!(live_prefix.observe("exe-2", prefix), None);
        assert_eq!(
            replay_detached_recorded_output(
                &live_prefix,
                "exe-2",
                &[suffix.to_owned(), suffix_terminal.to_owned()],
            ),
            Some(TerminalOutput::Completed {
                reason: "turn_completed",
                result_text: Some("Hello world.".to_owned()),
            }),
            "a truncated recorded suffix must combine with the live prefix"
        );
    }

    #[test]
    fn streamed_answer_deltas_preserve_boundary_whitespace_during_live_and_recovery() {
        let prefix = r#"{"type":"item.agentMessage.delta","threadId":"root-thread","turnId":"root-turn","itemId":"root-answer","delta":"Hello "}"#;
        let suffix = r#"{"method":"item/agentMessage/delta","params":{"threadId":"root-thread","turnId":"root-turn","itemId":"root-answer","delta":"world"}}"#;
        let terminal = r#"{"type":"turn.completed","threadId":"root-thread","turn":{"id":"root-turn","status":"completed"}}"#;
        let expected = Some(TerminalOutput::Completed {
            reason: "turn_completed",
            result_text: Some("Hello world".to_owned()),
        });

        let mut live = StdoutPumpState::default();
        live.set_authoritative_root_thread_id("exe-live", "root-thread");
        assert_eq!(live.observe("exe-live", prefix), None);
        assert_eq!(live.observe("exe-live", suffix), None);
        assert_eq!(live.observe("exe-live", terminal), expected);

        let first_word = r#"{"type":"item.agentMessage.delta","threadId":"root-thread","turnId":"root-turn","itemId":"root-answer","delta":"Hello"}"#;
        let whitespace = r#"{"type":"item.agentMessage.delta","threadId":"root-thread","turnId":"root-turn","itemId":"root-answer","delta":" \n"}"#;
        let mut whitespace_chunks = StdoutPumpState::default();
        whitespace_chunks.set_authoritative_root_thread_id("exe-whitespace", "root-thread");
        assert_eq!(
            whitespace_chunks.observe("exe-whitespace", first_word),
            None
        );
        assert_eq!(
            whitespace_chunks.observe("exe-whitespace", whitespace),
            None
        );
        assert_eq!(whitespace_chunks.observe("exe-whitespace", suffix), None);
        assert_eq!(
            whitespace_chunks.observe("exe-whitespace", terminal),
            Some(TerminalOutput::Completed {
                reason: "turn_completed",
                result_text: Some("Hello \nworld".to_owned()),
            }),
            "a whitespace-only delta chunk must remain byte-exact"
        );

        for (execution_id, delta, expected_text) in [
            (
                "exe-leading-trailing",
                r#"{"type":"item.agentMessage.delta","threadId":"root-thread","turnId":"root-turn","itemId":"root-answer","delta":" leading "}"#,
                " leading ",
            ),
            (
                "exe-whitespace-only",
                r#"{"type":"item.agentMessage.delta","threadId":"root-thread","turnId":"root-turn","itemId":"root-answer","delta":" \n\t"}"#,
                " \n\t",
            ),
        ] {
            let mut exact = StdoutPumpState::default();
            exact.set_authoritative_root_thread_id(execution_id, "root-thread");
            assert_eq!(exact.observe(execution_id, delta), None);
            assert_eq!(
                exact.observe(execution_id, terminal),
                Some(TerminalOutput::Completed {
                    reason: "turn_completed",
                    result_text: Some(expected_text.to_owned()),
                }),
                "bare completion must preserve accumulated delta bytes"
            );
        }

        let mut detached = StdoutPumpState::default();
        detached.set_authoritative_root_thread_id("exe-detached", "root-thread");
        assert_eq!(detached.observe("exe-detached", prefix), None);
        assert_eq!(
            replay_detached_recorded_output(
                &detached,
                "exe-detached",
                &[suffix.to_owned(), terminal.to_owned()],
            ),
            expected,
            "detached recovery must retain whitespace from the live prefix"
        );

        let mut detached_whitespace = StdoutPumpState::default();
        detached_whitespace
            .set_authoritative_root_thread_id("exe-detached-whitespace", "root-thread");
        let whitespace_only = r#"{"type":"item.agentMessage.delta","threadId":"root-thread","turnId":"root-turn","itemId":"root-answer","delta":" \n\t"}"#;
        assert_eq!(
            detached_whitespace.observe("exe-detached-whitespace", whitespace_only),
            None
        );
        assert_eq!(
            replay_detached_recorded_output(
                &detached_whitespace,
                "exe-detached-whitespace",
                &[terminal.to_owned()],
            ),
            Some(TerminalOutput::Completed {
                reason: "turn_completed",
                result_text: Some(" \n\t".to_owned()),
            }),
            "terminal-only detached recovery must preserve a whitespace-only live delta"
        );
    }

    #[test]
    fn legacy_root_repair_requires_recorded_root_then_persisted_child() {
        let root_then_child = vec![
            r#"{"method":"thread/started","params":{"thread":{"id":"root-thread"}}}"#.to_owned(),
            r#"{"method":"thread/started","params":{"thread":{"id":"child-thread"}}}"#.to_owned(),
        ];
        assert_eq!(
            legacy_corrupted_root_repair_candidate(&root_then_child, "child-thread"),
            Some("root-thread".to_owned())
        );
        assert_eq!(
            legacy_corrupted_root_repair_candidate(
                &[
                    r#"{"method":"thread/started","params":{"thread":{"id":"child-thread"}}}"#
                        .to_owned()
                ],
                "child-thread",
            ),
            None,
            "child-only later-turn history must preserve the durable root"
        );
    }

    #[test]
    fn steering_input_lines_forward_only_user_messages() {
        let thread_key = ThreadKey::parse("cli:test-steering").unwrap();
        let messages = vec![
            SessionMessageInput {
                client_message_id: None,
                role: MessageRole::User,
                parts: vec![json!({"type": "text", "text": "steer now"})],
                metadata: json!({"platform": "test"}),
            },
            SessionMessageInput {
                client_message_id: None,
                role: MessageRole::Assistant,
                parts: vec![json!({"type": "text", "text": "do not echo assistant"})],
                metadata: json!({}),
            },
        ];
        let message_ids = vec!["msg-user".to_owned(), "msg-assistant".to_owned()];

        let lines = steering_input_lines(&thread_key, &messages, &message_ids);
        assert_eq!(lines.len(), 1);

        let value: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
        assert_eq!(value["type"], "user");
        assert_eq!(value["thread_key"], "cli:test-steering");
        assert_eq!(value["trace_metadata"]["action"], "steer_active_execution");
        assert_eq!(value["trace_metadata"]["message_id"], "msg-user");
        assert_eq!(value["message"]["content"][0]["text"], "steer now");
    }

    #[test]
    fn harness_thread_id_is_extracted_from_thread_started_output() {
        assert_eq!(
            harness_thread_id_from_output_line(
                r#"{"type":"thread.started","thread_id":"codex-thread-1"}"#
            ),
            Some("codex-thread-1".to_owned())
        );
        assert_eq!(
            harness_thread_id_from_output_line(
                r#"{"type":"thread.started","threadId":"codex-thread-2"}"#
            ),
            Some("codex-thread-2".to_owned())
        );
        assert_eq!(
            harness_thread_id_from_output_line(
                r#"{"method":"thread/started","params":{"thread":{"id":"codex-thread-3"}}}"#
            ),
            Some("codex-thread-3".to_owned())
        );
        assert_eq!(
            harness_thread_id_from_output_line(r#"{"type":"turn.started","turn_id":"turn-1"}"#),
            None
        );
    }

    #[test]
    fn codex_workload_applies_mounts_to_sandbox_spec() {
        let workload = SandboxWorkloadMode::codex_app_server(
            "centaur-agent:latest",
            [("CENTAUR_API_URL".to_owned(), "http://api:8000".to_owned())],
            HarnessType::Codex,
        )
        .mount(
            Mount::new(
                MountKind::Bind {
                    source_path: "/host/github".to_owned(),
                },
                "/home/agent/github",
            )
            .read_only(),
        );
        let thread_key = ThreadKey::parse("chat:C123:1780000000.000000").unwrap();

        let spec = workload.spec(&thread_key, &HarnessType::Codex, None);

        assert_eq!(spec.mounts.len(), 1);
        assert_eq!(spec.mounts[0].target_path, "/home/agent/github");
        assert!(spec.mounts[0].read_only);
        assert_eq!(
            spec.mounts[0].kind,
            MountKind::Bind {
                source_path: "/host/github".to_owned(),
            }
        );
    }

    #[test]
    fn codex_workload_reflects_resolved_persona_in_sandbox_spec() {
        let workload = SandboxWorkloadMode::codex_app_server(
            "centaur-agent:latest",
            [("AGENT_PERSONA".to_owned(), "stale".to_owned())],
            HarnessType::Codex,
        );
        let thread_key = ThreadKey::parse("chat:C123:1780000000.000000").unwrap();
        let persona = test_persona_context("eng");

        let spec = workload.spec(&thread_key, &HarnessType::Codex, Some(&persona));

        assert_eq!(env_value(&spec, "AGENT_PERSONA"), Some("eng"));
        assert_eq!(env_value(&spec, "CENTAUR_PERSONA_ID"), Some("eng"));
        assert_eq!(
            env_value(&spec, "CENTAUR_PERSONA_PROMPT_HASH"),
            Some("sha256:prompt")
        );
        assert_eq!(
            env_value(&spec, "CENTAUR_PERSONA_SOURCE_REF"),
            Some("abc123")
        );
        assert_eq!(env_value(&workload.warm_spec(), "AGENT_PERSONA"), None);
    }

    #[test]
    fn codex_workload_does_not_inject_stale_continue_thread_id() {
        let workload = SandboxWorkloadMode::codex_app_server(
            "centaur-agent:latest",
            Vec::new(),
            HarnessType::Codex,
        );
        let thread_key = ThreadKey::parse("chat:C123:1780000000.000000").unwrap();

        let spec = workload.spec(&thread_key, &HarnessType::Codex, None);

        assert_eq!(
            spec.env
                .iter()
                .find(|env| env.name == "CODEX_CONTINUE_THREAD_ID")
                .map(|env| env.value.as_str()),
            None
        );
        assert_eq!(
            spec.env
                .iter()
                .find(|env| env.name == "AMP_CONTINUE_THREAD_ID")
                .map(|env| env.value.as_str()),
            None
        );
    }

    #[test]
    fn codex_warm_spec_starts_profileless() {
        let workload = SandboxWorkloadMode::codex_app_server(
            "centaur-agent:latest",
            [("CENTAUR_API_URL".to_owned(), "http://api:8000".to_owned())],
            HarnessType::Codex,
        );
        let thread_key = ThreadKey::parse("chat:C123:1780000000.000000").unwrap();

        let claimed_spec = workload.spec(&thread_key, &HarnessType::ClaudeCode, None);
        let warm_spec = workload.warm_spec();

        assert_eq!(
            env_value(&claimed_spec, "CENTAUR_THREAD_KEY"),
            Some(thread_key.as_str())
        );
        assert_eq!(env_value(&warm_spec, "CENTAUR_THREAD_KEY"), None);
    }

    #[test]
    fn warm_workload_key_ignores_claimed_thread_key() {
        let workload = SandboxWorkloadMode::codex_app_server(
            "centaur-agent:latest",
            [("CENTAUR_API_URL".to_owned(), "http://api:8000".to_owned())],
            HarnessType::Codex,
        );
        let first_thread_key = ThreadKey::parse("chat:C123:1780000000.000000").unwrap();
        let second_thread_key = ThreadKey::parse("chat:C456:1780000000.000001").unwrap();

        assert_ne!(
            sandbox_spec_key(&workload.spec(&first_thread_key, &HarnessType::ClaudeCode, None)),
            sandbox_spec_key(&workload.spec(&second_thread_key, &HarnessType::ClaudeCode, None))
        );
        assert_eq!(
            sandbox_spec_key(&workload.warm_spec()),
            sandbox_spec_key(&workload.warm_spec())
        );
    }

    #[test]
    fn codex_workload_pins_harness_via_container_args() {
        let workload = SandboxWorkloadMode::codex_app_server(
            "centaur-agent:latest",
            Vec::new(),
            HarnessType::Codex,
        );
        let thread_key = ThreadKey::parse("chat:C123:1780000000.000000").unwrap();

        let codex_spec = workload.spec(&thread_key, &HarnessType::Codex, None);
        let claude_spec = workload.spec(&thread_key, &HarnessType::ClaudeCode, None);
        let amp_spec = workload.spec(&thread_key, &HarnessType::Amp, None);

        assert_eq!(codex_spec.args, vec!["harness-server", "codex"]);
        assert_eq!(claude_spec.args, vec!["harness-server", "claude-code"]);
        assert_eq!(amp_spec.args, vec!["harness-server", "amp"]);
        // The image entrypoint must be preserved: only CMD is overridden.
        assert_eq!(codex_spec.command, None);
    }

    #[test]
    fn codex_workload_labels_session_sandbox_for_observability() {
        let workload = SandboxWorkloadMode::codex_app_server(
            "centaur-agent:latest",
            Vec::new(),
            HarnessType::Codex,
        );
        let thread_key = ThreadKey::parse("chat:C123:1780000000.000000").unwrap();

        let spec = workload.spec(&thread_key, &HarnessType::ClaudeCode, None);

        assert_eq!(
            spec.labels.get("centaur.ai/component").map(String::as_str),
            Some("session-sandbox")
        );
        assert_eq!(
            spec.labels.get("centaur.ai/harness").map(String::as_str),
            Some("claudecode")
        );
    }

    #[test]
    fn warm_spec_uses_workload_default_harness() {
        let workload = SandboxWorkloadMode::codex_app_server(
            "centaur-agent:latest",
            Vec::new(),
            HarnessType::Codex,
        );

        assert_eq!(
            workload.warm_spec().args,
            vec!["harness-server", "codex"],
            "warm sandboxes boot the configured default harness"
        );
        // A session on a different harness produces a different spec, so a
        // warm claim for it would hand over the wrong harness.
        let thread_key = ThreadKey::parse("chat:C123:1780000000.000000").unwrap();
        assert_eq!(
            workload
                .spec(&thread_key, &HarnessType::ClaudeCode, None)
                .args,
            vec!["harness-server", "claude-code"]
        );
    }

    #[test]
    fn input_line_with_session_context_enriches_json_objects() {
        let thread_key = ThreadKey::parse("chat:C123:1780000000.000000").unwrap();
        let trace = SessionTraceContext::new(&thread_key, None);

        let line = input_line_with_session_context(&thread_key, &trace, r#"{"type":"user"}"#);
        let value: Value = serde_json::from_str(&line).unwrap();

        assert_eq!(value["type"], "user");
        assert_eq!(value["thread_key"], thread_key.as_str());
        assert_eq!(value["trace_id"], trace.trace_id);
        // Without an OpenTelemetry layer there is no traceparent to forward.
        assert!(value.get("traceparent").is_none());
        assert!(value.get("session_context").is_none());
    }

    #[test]
    fn input_line_with_session_context_adds_slack_thread_context() {
        let thread_key = ThreadKey::parse("slack:T123:C123:1780000000.000000").unwrap();
        let trace = SessionTraceContext::new(&thread_key, None);

        let line = input_line_with_session_context(&thread_key, &trace, r#"{"type":"user"}"#);
        let value: Value = serde_json::from_str(&line).unwrap();

        assert_eq!(value["session_context"]["platform"], "slack");
        assert_eq!(value["session_context"]["slack"]["channel_id"], "C123");
        assert_eq!(
            value["session_context"]["slack"]["thread_ts"],
            "1780000000.000000"
        );
    }

    #[test]
    fn input_line_with_session_context_adds_discord_thread_context() {
        let thread_key = ThreadKey::parse("discord:111:222:333").unwrap();
        let trace = SessionTraceContext::new(&thread_key, None);

        let line = input_line_with_session_context(&thread_key, &trace, r#"{"type":"user"}"#);
        let value: Value = serde_json::from_str(&line).unwrap();

        assert_eq!(value["session_context"]["platform"], "discord");
        assert_eq!(value["session_context"]["discord"]["guild_id"], "111");
        assert_eq!(value["session_context"]["discord"]["channel_id"], "222");
        assert_eq!(value["session_context"]["discord"]["thread_id"], "333");
        assert!(value["session_context"].get("slack").is_none());
    }

    #[test]
    fn input_line_with_session_context_adds_linear_thread_context() {
        let thread_key = ThreadKey::parse("linear:ISSUE:s:SESS").unwrap();
        let trace = SessionTraceContext::new(&thread_key, None);

        let line = input_line_with_session_context(&thread_key, &trace, r#"{"type":"user"}"#);
        let value: Value = serde_json::from_str(&line).unwrap();

        assert_eq!(value["session_context"]["platform"], "linear");
        assert_eq!(value["session_context"]["linear"]["issue_id"], "ISSUE");
        assert_eq!(
            value["session_context"]["linear"]["agent_session_id"],
            "SESS"
        );
        // No comment in this key, so the optional field is omitted entirely.
        assert!(
            value["session_context"]["linear"]
                .get("comment_id")
                .is_none()
        );
    }

    #[test]
    fn input_line_with_session_context_adds_github_thread_context() {
        let thread_key = ThreadKey::parse("github:0xSplits/centaur:704:rc:99").unwrap();
        let trace = SessionTraceContext::new(&thread_key, None);

        let line = input_line_with_session_context(&thread_key, &trace, r#"{"type":"user"}"#);
        let value: Value = serde_json::from_str(&line).unwrap();

        assert_eq!(value["session_context"]["platform"], "github");
        assert_eq!(value["session_context"]["github"]["owner"], "0xSplits");
        assert_eq!(value["session_context"]["github"]["repo"], "centaur");
        assert_eq!(value["session_context"]["github"]["number"], 704);
        assert_eq!(value["session_context"]["github"]["kind"], "pr");
        assert_eq!(value["session_context"]["github"]["review_comment_id"], 99);
    }

    #[test]
    fn input_line_with_session_context_preserves_existing_session_context() {
        let thread_key = ThreadKey::parse("slack:T123:C123:1780000000.000000").unwrap();
        let trace = SessionTraceContext::new(&thread_key, None);

        let line = input_line_with_session_context(
            &thread_key,
            &trace,
            r#"{"type":"user","session_context":{"requester":{"github_handle":"@ada"},"platform":"custom"}}"#,
        );
        let value: Value = serde_json::from_str(&line).unwrap();

        assert_eq!(
            value["session_context"]["requester"]["github_handle"],
            "@ada"
        );
        assert_eq!(value["session_context"]["platform"], "custom");
        assert_eq!(value["session_context"]["slack"]["channel_id"], "C123");
    }

    #[test]
    fn input_line_with_session_context_preserves_existing_fields_and_non_json() {
        let thread_key = ThreadKey::parse("chat:C123:1780000000.000000").unwrap();
        let trace = SessionTraceContext {
            trace_id: thread_trace_id(&thread_key),
            traceparent: Some("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01".to_owned()),
        };

        let line = input_line_with_session_context(
            &thread_key,
            &trace,
            r#"{"type":"user","thread_key":"chat:existing","trace_id":"caller-trace"}"#,
        );
        let value: Value = serde_json::from_str(&line).unwrap();

        assert_eq!(value["thread_key"], "chat:existing");
        assert_eq!(value["trace_id"], "caller-trace");
        assert_eq!(
            value["traceparent"],
            "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
        );
        assert_eq!(
            input_line_with_session_context(&thread_key, &trace, "raw"),
            "raw"
        );
    }

    #[test]
    fn input_line_prepends_discord_chat_surface_note_to_user_content() {
        let thread_key = ThreadKey::parse("discord:111:222:333").unwrap();
        let trace = SessionTraceContext::new(&thread_key, None);

        let line = input_line_with_session_context(
            &thread_key,
            &trace,
            r#"{"type":"user","message":{"role":"user","content":[{"type":"text","text":"hi"}]}}"#,
        );
        let value: Value = serde_json::from_str(&line).unwrap();
        let content = value["message"]["content"].as_array().unwrap();

        // The note is prepended ahead of the original parts, which are preserved.
        assert_eq!(content.len(), 2);
        let note = content[0]["text"].as_str().unwrap();
        assert!(note.contains("Discord"));
        assert!(note.contains("222"));
        assert_eq!(content[1]["text"], "hi");
    }

    #[test]
    fn input_line_prepends_slack_chat_surface_note_to_user_content() {
        let thread_key = ThreadKey::parse("slack:C123:123.456").unwrap();
        let trace = SessionTraceContext::new(&thread_key, None);

        let line = input_line_with_session_context(
            &thread_key,
            &trace,
            r#"{"type":"user","message":{"role":"user","content":[{"type":"text","text":"hi"}]}}"#,
        );
        let value: Value = serde_json::from_str(&line).unwrap();
        let content = value["message"]["content"].as_array().unwrap();

        assert_eq!(content.len(), 2);
        assert!(content[0]["text"].as_str().unwrap().contains("Slack"));
        assert_eq!(content[1]["text"], "hi");
    }

    #[test]
    fn input_line_prepends_linear_chat_surface_note_to_user_content() {
        let thread_key = ThreadKey::parse("linear:ISSUE:s:SESS").unwrap();
        let trace = SessionTraceContext::new(&thread_key, None);

        let line = input_line_with_session_context(
            &thread_key,
            &trace,
            r#"{"type":"user","message":{"role":"user","content":[{"type":"text","text":"hi"}]}}"#,
        );
        let value: Value = serde_json::from_str(&line).unwrap();
        let content = value["message"]["content"].as_array().unwrap();

        assert_eq!(content.len(), 2);
        let note = content[0]["text"].as_str().unwrap();
        assert!(note.contains("Linear"));
        assert!(note.contains("ISSUE"));
        assert_eq!(content[1]["text"], "hi");
    }

    #[test]
    fn input_line_prepends_github_chat_surface_note_to_user_content() {
        let thread_key = ThreadKey::parse("github:0xSplits/centaur:issue:12").unwrap();
        let trace = SessionTraceContext::new(&thread_key, None);

        let line = input_line_with_session_context(
            &thread_key,
            &trace,
            r#"{"type":"user","message":{"role":"user","content":[{"type":"text","text":"hi"}]}}"#,
        );
        let value: Value = serde_json::from_str(&line).unwrap();
        let content = value["message"]["content"].as_array().unwrap();

        assert_eq!(content.len(), 2);
        let note = content[0]["text"].as_str().unwrap();
        assert!(note.contains("GitHub"));
        assert!(note.contains("0xSplits/centaur#12"));
        assert_eq!(content[1]["text"], "hi");
    }

    #[test]
    fn input_line_leaves_content_untouched_without_a_chat_destination() {
        // A non-platform thread key resolves to no destination, so nothing is added.
        let thread_key = ThreadKey::parse("cli:test").unwrap();
        let trace = SessionTraceContext::new(&thread_key, None);

        let line = input_line_with_session_context(
            &thread_key,
            &trace,
            r#"{"type":"user","message":{"role":"user","content":[{"type":"text","text":"hi"}]}}"#,
        );
        let value: Value = serde_json::from_str(&line).unwrap();
        let content = value["message"]["content"].as_array().unwrap();

        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["text"], "hi");
    }

    #[test]
    fn thread_trace_id_is_deterministic_per_thread() {
        let thread_key = ThreadKey::parse("chat:C123:1780000000.000000").unwrap();
        let other = ThreadKey::parse("chat:C456:1780000000.000000").unwrap();

        assert_eq!(thread_trace_id(&thread_key), thread_trace_id(&thread_key));
        assert_ne!(thread_trace_id(&thread_key), thread_trace_id(&other));
        // The wrapper parses this with uuid.UUID(...): must stay a canonical UUID.
        assert!(uuid::Uuid::parse_str(&thread_trace_id(&thread_key)).is_ok());
        assert_eq!(
            thread_trace_parent_span_id(&thread_key),
            thread_trace_parent_span_id(&thread_key)
        );
        assert_ne!(
            thread_trace_parent_span_id(&thread_key),
            thread_trace_parent_span_id(&other)
        );
        assert_eq!(thread_trace_parent_span_id(&thread_key).len(), 16);
        assert_ne!(thread_trace_parent_span_id(&thread_key), "0000000000000000");
    }

    #[test]
    fn traced_input_timeout_is_capped_below_long_consent_deadlines() {
        assert_eq!(
            metadata_trace_write_timeout(Duration::from_secs(24 * 60 * 60)),
            Duration::from_secs(30)
        );
        assert_eq!(
            metadata_trace_write_timeout(Duration::from_secs(7)),
            Duration::from_secs(7)
        );
    }

    #[test]
    fn proxy_labels_from_session_metadata_use_centaur_slack_keys() {
        let thread_key = ThreadKey::parse("slack:T123:C123:1700000000.000000").unwrap();
        let labels = proxy_labels_from_session_metadata(
            &thread_key,
            &json!({
                "slack_user_id": "U123",
                "slack_team_id": "T123",
                "slack_channel_id": "C456",
                "slack_user_email": "ada@example.com"
            }),
        );

        assert_eq!(
            labels,
            BTreeMap::from([
                ("centaur.slack_channel_id".to_owned(), "C456".to_owned()),
                ("centaur.slack_team_id".to_owned(), "T123".to_owned()),
                ("centaur.slack_user_id".to_owned(), "U123".to_owned()),
            ])
        );
    }

    #[test]
    fn proxy_labels_from_session_metadata_does_not_infer_slack_channel_for_linear_keys() {
        let thread_key = ThreadKey::parse("linear:CEN-123:s:agent-session").unwrap();
        let labels = proxy_labels_from_session_metadata(
            &thread_key,
            &json!({
                "slack_user_id": "U123",
                "slack_team_id": "T123",
            }),
        );

        assert_eq!(
            labels,
            BTreeMap::from([
                ("centaur.slack_team_id".to_owned(), "T123".to_owned()),
                ("centaur.slack_user_id".to_owned(), "U123".to_owned()),
            ])
        );
    }

    fn session_with_sandbox(sandbox_id: &str) -> Session {
        let thread_key = ThreadKey::parse("cli:test-idle").unwrap();
        let now = OffsetDateTime::now_utc();
        Session {
            thread_key,
            title: None,
            sandbox_id: Some(sandbox_id.to_owned()),
            sandbox_capabilities: None,
            harness_type: HarnessType::Codex,
            harness_thread_id: None,
            persona_id: None,
            status: SessionStatus::Idle,
            iron_control_principal: None,
            proxy_labels: BTreeMap::new(),
            sandbox_last_active_at: Some(now),
            created_at: now,
            updated_at: now,
        }
    }

    fn session_execution(
        execution_id: &str,
        status: ExecutionStatus,
        metadata: serde_json::Value,
    ) -> SessionExecution {
        let thread_key = ThreadKey::parse("cli:test-idle").unwrap();
        let now = OffsetDateTime::now_utc();
        SessionExecution {
            execution_id: execution_id.to_owned(),
            idempotency_key: None,
            thread_key,
            status,
            metadata,
            error: None,
            created_at: now,
            updated_at: now,
            started_at: Some(now),
            completed_at: Some(now),
        }
    }

    fn env_value<'a>(spec: &'a SandboxSpec, name: &str) -> Option<&'a str> {
        spec.env
            .iter()
            .find(|env| env.name == name)
            .map(|env| env.value.as_str())
    }

    fn test_persona_context(persona_id: &str) -> PersonaContext {
        PersonaContext {
            persona_id: persona_id.to_owned(),
            source_root: "/repo/tools".to_owned(),
            source_path: format!("/repo/tools/personas/{persona_id}"),
            source_ref: Some("abc123".to_owned()),
            prompt_hash: "sha256:prompt".to_owned(),
            defaulted: false,
            overlay_chain: vec!["/repo/tools".to_owned()],
        }
    }
}

/// Integration tests for orphaned-execution adoption. They need a real
/// Postgres; set `SESSION_RUNTIME_TEST_DATABASE_URL` or the CI-standard
/// `SESSION_SQLX_TEST_DATABASE_URL` to run them.
#[cfg(test)]
mod adoption_tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use centaur_sandbox_core::{ObservedSandbox, SandboxHandle, SandboxIo, SandboxResult};
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, DuplexStream};

    use super::*;

    /// The adoption scan is database-wide, so concurrently running tests
    /// would adopt each other's executions. Serialize the module; every test
    /// fully terminalizes its own executions before releasing the lock.
    static TEST_LOCK: Mutex<()> = Mutex::const_new(());

    type ProxyEnsure = (String, String, BTreeMap<String, String>);

    struct MockBackend {
        ios: Mutex<VecDeque<SandboxIo>>,
        recorded_output: std::sync::Mutex<Vec<String>>,
        block_recorded_output: AtomicBool,
        recorded_output_read_started: tokio::sync::Notify,
        recorded_output_read_release: tokio::sync::Notify,
        recorded_output_read_count: AtomicUsize,
        block_open_io: AtomicBool,
        open_io_started: tokio::sync::Notify,
        open_io_release: tokio::sync::Notify,
        block_status: AtomicBool,
        status_started: tokio::sync::Notify,
        status_release: tokio::sync::Notify,
        block_pause: AtomicBool,
        pause_started: tokio::sync::Notify,
        pause_release: tokio::sync::Notify,
        block_running_fence: AtomicBool,
        running_fence_started: tokio::sync::Notify,
        running_fence_release: tokio::sync::Notify,
        running_fence_count: AtomicUsize,
        open_count: AtomicUsize,
        status: std::sync::Mutex<SandboxStatus>,
        observed_statuses: std::sync::Mutex<BTreeMap<String, SandboxStatus>>,
        observed_resource_uids: std::sync::Mutex<BTreeMap<String, String>>,
        create_id: String,
        created_specs: std::sync::Mutex<Vec<SandboxSpec>>,
        resume_fails: AtomicBool,
        observe_fails: AtomicBool,
        stop_fails: AtomicBool,
        stop_preserves_status: AtomicBool,
        stop_delay: std::sync::Mutex<Option<Duration>>,
        stopped: std::sync::Mutex<Vec<String>>,
        proxy_ensures: std::sync::Mutex<Vec<ProxyEnsure>>,
        missing_on_stop: std::sync::Mutex<BTreeSet<String>>,
    }

    impl MockBackend {
        fn new(status: SandboxStatus, recorded_output: Vec<String>) -> Self {
            Self {
                ios: Mutex::new(VecDeque::new()),
                recorded_output: std::sync::Mutex::new(recorded_output),
                block_recorded_output: AtomicBool::new(false),
                recorded_output_read_started: tokio::sync::Notify::new(),
                recorded_output_read_release: tokio::sync::Notify::new(),
                recorded_output_read_count: AtomicUsize::new(0),
                block_open_io: AtomicBool::new(false),
                open_io_started: tokio::sync::Notify::new(),
                open_io_release: tokio::sync::Notify::new(),
                block_status: AtomicBool::new(false),
                status_started: tokio::sync::Notify::new(),
                status_release: tokio::sync::Notify::new(),
                block_pause: AtomicBool::new(false),
                pause_started: tokio::sync::Notify::new(),
                pause_release: tokio::sync::Notify::new(),
                block_running_fence: AtomicBool::new(false),
                running_fence_started: tokio::sync::Notify::new(),
                running_fence_release: tokio::sync::Notify::new(),
                running_fence_count: AtomicUsize::new(0),
                open_count: AtomicUsize::new(0),
                status: std::sync::Mutex::new(status),
                observed_statuses: std::sync::Mutex::new(BTreeMap::new()),
                observed_resource_uids: std::sync::Mutex::new(BTreeMap::new()),
                create_id: "mock-sbx".to_owned(),
                created_specs: std::sync::Mutex::new(Vec::new()),
                resume_fails: AtomicBool::new(false),
                observe_fails: AtomicBool::new(false),
                stop_fails: AtomicBool::new(false),
                stop_preserves_status: AtomicBool::new(false),
                stop_delay: std::sync::Mutex::new(None),
                stopped: std::sync::Mutex::new(Vec::new()),
                proxy_ensures: std::sync::Mutex::new(Vec::new()),
                missing_on_stop: std::sync::Mutex::new(BTreeSet::new()),
            }
        }

        async fn push_io(&self, io: SandboxIo) {
            self.ios.lock().await.push_back(io);
        }

        fn opens(&self) -> usize {
            self.open_count.load(Ordering::SeqCst)
        }

        fn set_recorded_output(&self, recorded_output: Vec<String>) {
            *self.recorded_output.lock().unwrap() = recorded_output;
        }

        fn block_next_recorded_output_read(&self) {
            self.block_recorded_output.store(true, Ordering::SeqCst);
        }

        async fn wait_for_recorded_output_read(&self) {
            self.recorded_output_read_started.notified().await;
        }

        fn release_recorded_output_read(&self) {
            self.recorded_output_read_release.notify_one();
        }

        fn recorded_output_reads(&self) -> usize {
            self.recorded_output_read_count.load(Ordering::SeqCst)
        }

        fn block_next_open_io(&self) {
            self.block_open_io.store(true, Ordering::SeqCst);
        }

        async fn wait_for_open_io(&self) {
            self.open_io_started.notified().await;
        }

        fn release_open_io(&self) {
            self.open_io_release.notify_one();
        }

        fn block_next_status(&self) {
            self.block_status.store(true, Ordering::SeqCst);
        }

        async fn wait_for_status(&self) {
            self.status_started.notified().await;
        }

        fn release_status(&self) {
            self.status_release.notify_one();
        }

        fn block_next_pause(&self) {
            self.block_pause.store(true, Ordering::SeqCst);
        }

        async fn wait_for_pause(&self) {
            self.pause_started.notified().await;
        }

        fn release_pause(&self) {
            self.pause_release.notify_one();
        }

        fn block_next_running_fence(&self) {
            self.block_running_fence.store(true, Ordering::SeqCst);
        }

        async fn wait_for_running_fence(&self) {
            self.running_fence_started.notified().await;
        }

        fn release_running_fence(&self) {
            self.running_fence_release.notify_one();
        }

        fn running_fence_count(&self) -> usize {
            self.running_fence_count.load(Ordering::SeqCst)
        }

        fn set_status(&self, status: SandboxStatus) {
            *self.status.lock().unwrap() = status;
        }

        fn set_observed_status(&self, sandbox_id: &str, status: SandboxStatus) {
            self.observed_statuses
                .lock()
                .unwrap()
                .insert(sandbox_id.to_owned(), status);
        }

        fn set_observed_resource_uid(&self, sandbox_id: &str, resource_uid: &str) {
            self.observed_resource_uids
                .lock()
                .unwrap()
                .insert(sandbox_id.to_owned(), resource_uid.to_owned());
        }

        fn clear_observed_resource_uid(&self, sandbox_id: &str) {
            self.observed_resource_uids
                .lock()
                .unwrap()
                .remove(sandbox_id);
        }

        fn status_of(&self, sandbox_id: &str) -> Option<SandboxStatus> {
            self.observed_statuses
                .lock()
                .unwrap()
                .get(sandbox_id)
                .cloned()
        }

        fn fail_resume(&self) {
            self.resume_fails.store(true, Ordering::SeqCst);
        }

        fn fail_observe(&self) {
            self.observe_fails.store(true, Ordering::SeqCst);
        }

        fn allow_observe(&self) {
            self.observe_fails.store(false, Ordering::SeqCst);
        }

        fn fail_stop(&self) {
            self.stop_fails.store(true, Ordering::SeqCst);
        }

        fn allow_stop(&self) {
            self.stop_fails.store(false, Ordering::SeqCst);
        }

        fn preserve_status_after_stop(&self, preserve: bool) {
            self.stop_preserves_status.store(preserve, Ordering::SeqCst);
        }

        fn set_stop_delay(&self, delay: Option<Duration>) {
            *self.stop_delay.lock().unwrap() = delay;
        }

        fn mark_stop_missing(&self, sandbox_id: &str) {
            self.missing_on_stop
                .lock()
                .unwrap()
                .insert(sandbox_id.to_owned());
        }

        fn stopped(&self) -> Vec<String> {
            self.stopped.lock().unwrap().clone()
        }

        fn proxy_ensures(&self) -> Vec<ProxyEnsure> {
            self.proxy_ensures.lock().unwrap().clone()
        }

        fn created_specs(&self) -> Vec<SandboxSpec> {
            self.created_specs.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl SandboxBackend for MockBackend {
        fn name(&self) -> &'static str {
            "mock"
        }

        async fn create(&self, spec: SandboxSpec) -> SandboxResult<SandboxHandle> {
            self.created_specs.lock().unwrap().push(spec);
            self.set_observed_status(&self.create_id, SandboxStatus::Running);
            let resource_uid = self
                .observed_resource_uids
                .lock()
                .unwrap()
                .entry(self.create_id.clone())
                .or_insert_with(|| format!("mock-uid-{}", Uuid::new_v4()))
                .clone();
            Ok(
                SandboxHandle::new(SandboxId::new(self.create_id.clone()), "mock")
                    .with_resource_uid(Some(resource_uid)),
            )
        }

        async fn open_io(&self, _id: &SandboxId) -> SandboxResult<SandboxIo> {
            self.open_count.fetch_add(1, Ordering::SeqCst);
            if self.block_open_io.swap(false, Ordering::SeqCst) {
                self.open_io_started.notify_one();
                self.open_io_release.notified().await;
            }
            let io = self
                .ios
                .lock()
                .await
                .pop_front()
                .ok_or_else(|| SandboxError::io("mock backend has no more ios"))?;
            Ok(io.with_resource_uid(
                self.observed_resource_uids
                    .lock()
                    .unwrap()
                    .get(_id.as_str())
                    .cloned(),
            ))
        }

        async fn read_output_since(
            &self,
            _id: &SandboxId,
            _since: Option<SystemTime>,
        ) -> SandboxResult<Vec<String>> {
            self.recorded_output_read_count
                .fetch_add(1, Ordering::SeqCst);
            if self.block_recorded_output.swap(false, Ordering::SeqCst) {
                self.recorded_output_read_started.notify_one();
                self.recorded_output_read_release.notified().await;
            }
            Ok(self.recorded_output.lock().unwrap().clone())
        }

        async fn status(&self, _id: &SandboxId) -> SandboxResult<SandboxStatus> {
            if self.block_status.swap(false, Ordering::SeqCst) {
                self.status_started.notify_one();
                self.status_release.notified().await;
            }
            if let Some(status) = self.status_of(_id.as_str()) {
                return Ok(status);
            }
            Ok(self.status.lock().unwrap().clone())
        }

        async fn observe(&self, id: &SandboxId) -> SandboxResult<ObservedSandbox> {
            if self.observe_fails.load(Ordering::SeqCst) {
                return Err(SandboxError::backend("mock observe failure"));
            }
            let status = self.status(id).await?;
            Ok(
                ObservedSandbox::new(id.clone(), "mock", status).with_resource_uid(
                    self.observed_resource_uids
                        .lock()
                        .unwrap()
                        .get(id.as_str())
                        .cloned(),
                ),
            )
        }

        async fn list_observed(&self) -> SandboxResult<Vec<ObservedSandbox>> {
            Ok(self
                .observed_statuses
                .lock()
                .unwrap()
                .iter()
                .map(|(id, status)| {
                    ObservedSandbox::new(id.as_str(), "mock", status.clone()).with_resource_uid(
                        self.observed_resource_uids.lock().unwrap().get(id).cloned(),
                    )
                })
                .collect())
        }

        async fn stop(&self, id: &SandboxId) -> SandboxResult<()> {
            let delay = *self.stop_delay.lock().unwrap();
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            if self.stop_fails.load(Ordering::SeqCst) {
                return Err(SandboxError::backend("mock stop failure"));
            }
            if self.missing_on_stop.lock().unwrap().contains(id.as_str()) {
                return Err(SandboxError::NotFound(id.as_str().to_owned()));
            }
            self.stopped.lock().unwrap().push(id.as_str().to_owned());
            if !self.stop_preserves_status.load(Ordering::SeqCst) {
                self.set_observed_status(id.as_str(), SandboxStatus::Gone);
            }
            Ok(())
        }

        async fn stop_exact(
            &self,
            id: &SandboxId,
            expected_resource_uid: Option<&str>,
        ) -> SandboxResult<()> {
            if let Some(expected_resource_uid) = expected_resource_uid
                && self
                    .observed_resource_uids
                    .lock()
                    .unwrap()
                    .get(id.as_str())
                    .map(String::as_str)
                    != Some(expected_resource_uid)
            {
                return Err(SandboxError::backend(
                    "mock exact stop resource UID did not match",
                ));
            }
            self.stop(id).await
        }

        async fn ensure_iron_control_proxy_resources(
            &self,
            id: &SandboxId,
            principal_id: &str,
            labels: &BTreeMap<String, String>,
        ) -> SandboxResult<()> {
            self.proxy_ensures.lock().unwrap().push((
                id.as_str().to_owned(),
                principal_id.to_owned(),
                labels.clone(),
            ));
            Ok(())
        }

        async fn pause(&self, _id: &SandboxId) -> SandboxResult<()> {
            if self.block_pause.swap(false, Ordering::SeqCst) {
                self.pause_started.notify_one();
                self.pause_release.notified().await;
            }
            self.set_observed_status(_id.as_str(), SandboxStatus::Suspended);
            Ok(())
        }

        async fn pause_exact(
            &self,
            id: &SandboxId,
            expected_resource_uid: Option<&str>,
        ) -> SandboxResult<()> {
            if self
                .observed_resource_uids
                .lock()
                .unwrap()
                .get(id.as_str())
                .map(String::as_str)
                != expected_resource_uid
            {
                return Err(SandboxError::backend(
                    "mock exact pause resource UID did not match",
                ));
            }
            self.pause(id).await
        }

        async fn resume(&self, _id: &SandboxId) -> SandboxResult<()> {
            if self.resume_fails.load(Ordering::SeqCst) {
                return Err(SandboxError::NotFound(_id.as_str().to_owned()));
            }
            self.set_observed_status(_id.as_str(), SandboxStatus::Running);
            Ok(())
        }

        async fn resume_exact(
            &self,
            id: &SandboxId,
            expected_resource_uid: Option<&str>,
        ) -> SandboxResult<()> {
            if self
                .observed_resource_uids
                .lock()
                .unwrap()
                .get(id.as_str())
                .map(String::as_str)
                != expected_resource_uid
            {
                return Err(SandboxError::backend(
                    "mock exact resume resource UID did not match",
                ));
            }
            self.resume(id).await
        }

        async fn ensure_running_exact(
            &self,
            id: &SandboxId,
            expected_resource_uid: &str,
            _fence_nonce: &str,
        ) -> SandboxResult<()> {
            self.running_fence_count.fetch_add(1, Ordering::SeqCst);
            if self.resume_fails.load(Ordering::SeqCst) {
                return Err(SandboxError::backend("mock running fence failure"));
            }
            if self.block_running_fence.swap(false, Ordering::SeqCst) {
                self.running_fence_started.notify_one();
                self.running_fence_release.notified().await;
            }
            if self
                .observed_resource_uids
                .lock()
                .unwrap()
                .get(id.as_str())
                .map(String::as_str)
                != Some(expected_resource_uid)
            {
                return Err(SandboxError::backend(
                    "mock running fence resource UID did not match",
                ));
            }
            self.set_observed_status(id.as_str(), SandboxStatus::Running);
            Ok(())
        }
    }

    fn mock_io() -> (SandboxIo, DuplexStream, DuplexStream) {
        let (stdin_near, stdin_far) = tokio::io::duplex(64 * 1024);
        let (stdout_near, stdout_far) = tokio::io::duplex(64 * 1024);
        let (stderr_near, _stderr_far) = tokio::io::duplex(1024);
        let io = SandboxIo::new(
            Box::pin(stdin_near),
            Box::pin(stdout_near),
            Box::pin(stderr_near),
        );
        (io, stdout_far, stdin_far)
    }

    fn completed_output_lines(result_text: &str) -> Vec<String> {
        vec![
            json!({
                "type": "item.completed",
                "item": {
                    "id": "msg-1",
                    "type": "agentMessage",
                    "text": result_text,
                    "phase": "final_answer"
                }
            })
            .to_string(),
            json!({"type": "turn.completed", "turn": {"id": "turn-1", "status": "completed"}})
                .to_string(),
        ]
    }

    fn completed_output_bytes(result_text: &str) -> Vec<u8> {
        let mut output = completed_output_lines(result_text).join("\n");
        output.push('\n');
        output.into_bytes()
    }

    const ROOT_THREAD_STARTED_LINE: &str =
        r#"{"method":"thread/started","params":{"thread":{"id":"root-thread"}}}"#;
    const ROOT_TURN_STARTED_LINE: &str = r#"{"method":"turn/started","params":{"threadId":"root-thread","turn":{"id":"root-turn"}}}"#;
    const ROOT_ANSWER_LINE: &str = r#"{"method":"item/completed","params":{"threadId":"root-thread","turnId":"root-turn","item":{"id":"root-answer","type":"agentMessage","phase":"final_answer","text":"Root answer."}}}"#;
    const ROOT_COMPLETED_LINE: &str = r#"{"method":"turn/completed","params":{"threadId":"root-thread","turn":{"id":"root-turn","status":"completed"}}}"#;
    const CHILD_THREAD_STARTED_LINE: &str =
        r#"{"method":"thread/started","params":{"thread":{"id":"child-thread"}}}"#;
    const CHILD_TURN_STARTED_LINE: &str = r#"{"method":"turn/started","params":{"threadId":"child-thread","turn":{"id":"child-turn"}}}"#;
    const CHILD_ANSWER_LINE: &str = r#"{"method":"item/completed","params":{"threadId":"child-thread","turnId":"child-turn","item":{"id":"child-answer","type":"agentMessage","phase":"final_answer","text":"Child answer."}}}"#;
    const CHILD_COMPLETED_LINE: &str = r#"{"method":"turn/completed","params":{"threadId":"child-thread","turn":{"id":"child-turn","status":"completed"}}}"#;
    const ROOT_START_LINES: [&str; 2] = [ROOT_THREAD_STARTED_LINE, ROOT_TURN_STARTED_LINE];
    const ROOT_TERMINAL_LINES: [&str; 2] = [ROOT_ANSWER_LINE, ROOT_COMPLETED_LINE];
    const CHILD_LINES: [&str; 4] = [
        CHILD_THREAD_STARTED_LINE,
        CHILD_TURN_STARTED_LINE,
        CHILD_ANSWER_LINE,
        CHILD_COMPLETED_LINE,
    ];

    fn output_bytes(lines: &[&str]) -> Vec<u8> {
        let mut output = lines.join("\n");
        output.push('\n');
        output.into_bytes()
    }

    fn owned_output_lines(lines: &[&str]) -> Vec<String> {
        lines.iter().map(|line| (*line).to_owned()).collect()
    }

    async fn test_store() -> Option<PgSessionStore> {
        let Ok(url) = std::env::var("SESSION_RUNTIME_TEST_DATABASE_URL")
            .or_else(|_| std::env::var("SESSION_SQLX_TEST_DATABASE_URL"))
        else {
            eprintln!(
                "skipping: SESSION_RUNTIME_TEST_DATABASE_URL and \
                 SESSION_SQLX_TEST_DATABASE_URL are not set"
            );
            return None;
        };
        let store = PgSessionStore::connect(&url)
            .await
            .expect("connect test db");
        store.run_migrations().await.expect("run migrations");
        Some(store)
    }

    async fn orphaned_execution(
        store: &PgSessionStore,
        thread_key: &ThreadKey,
        sandbox_id: Option<&str>,
        running: bool,
    ) -> String {
        store
            .create_or_get_session(
                thread_key,
                &HarnessType::Codex,
                None,
                json!({}),
                Default::default(),
            )
            .await
            .expect("create session");
        if let Some(sandbox_id) = sandbox_id {
            store
                .update_sandbox_id(thread_key, Some(sandbox_id))
                .await
                .expect("set legacy sandbox id");
        }
        let created = store
            .create_execution(thread_key, None, json!({}))
            .await
            .expect("create execution");
        let execution_id = created.execution.execution_id;
        if running {
            store
                .mark_execution_running(&execution_id)
                .await
                .expect("mark running");
        }
        execution_id
    }

    /// Ages an execution row past `PRE_SANDBOX_ORPHAN_GRACE` so adoption treats it
    /// as a genuine orphan instead of a young row racing a live execute.
    async fn backdate_execution(store: &PgSessionStore, execution_id: &str, seconds: f64) {
        let result = sqlx::query(
            "update session_executions \
             set created_at = created_at - make_interval(secs => $2), \
                 started_at = started_at - make_interval(secs => $2) \
             where execution_id = $1",
        )
        .bind(execution_id)
        .bind(seconds)
        .execute(store.pool())
        .await
        .expect("backdate execution");
        assert_eq!(result.rows_affected(), 1, "expected to backdate one row");
    }

    /// Expires an execution's stdout-owner lease in place, simulating an
    /// owner that died without releasing, deterministically (no sleeps
    /// racing real lease TTLs).
    async fn expire_stdout_lease(store: &PgSessionStore, execution_id: &str) {
        let result = sqlx::query(
            "update session_executions \
             set stdout_owner_lease_expires_at = now() - interval '1 second' \
             where execution_id = $1",
        )
        .bind(execution_id)
        .execute(store.pool())
        .await
        .expect("expire stdout lease");
        assert_eq!(result.rows_affected(), 1, "expected to expire one lease");
    }

    async fn wait_for_event(store: &PgSessionStore, thread_key: &ThreadKey, event_type: &str) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let events = store
                .list_events_after(thread_key, 0, None, 1000)
                .await
                .expect("list events");
            if events.iter().any(|event| event.event_type == event_type) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {event_type}"
            );
            sleep(Duration::from_millis(25)).await;
        }
    }

    async fn wait_for_execution_event(
        store: &PgSessionStore,
        thread_key: &ThreadKey,
        execution_id: &str,
        event_type: &str,
    ) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let events = store
                .list_events_after(thread_key, 0, Some(execution_id), 1000)
                .await
                .expect("list execution events");
            if events.iter().any(|event| event.event_type == event_type) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {event_type} on {execution_id}"
            );
            sleep(Duration::from_millis(25)).await;
        }
    }

    async fn wait_for_output_line(
        store: &PgSessionStore,
        thread_key: &ThreadKey,
        expected_line: &str,
    ) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let events = store
                .list_events_after(thread_key, 0, None, 1000)
                .await
                .expect("list events");
            if events.iter().any(|event| {
                event.event_type == SESSION_OUTPUT_LINE_EVENT
                    && event.payload.as_str() == Some(expected_line)
            }) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for output line {expected_line}"
            );
            sleep(Duration::from_millis(25)).await;
        }
    }

    async fn wait_for_session_title(
        store: &PgSessionStore,
        thread_key: &ThreadKey,
        expected: &str,
    ) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let session = store.get_session(thread_key).await.expect("get session");
            if session.title.as_deref() == Some(expected) {
                return;
            }
            assert!(Instant::now() < deadline, "timed out waiting for title");
            sleep(Duration::from_millis(25)).await;
        }
    }

    async fn events(store: &PgSessionStore, thread_key: &ThreadKey) -> Vec<SessionEvent> {
        store
            .list_events_after(thread_key, 0, None, 1000)
            .await
            .expect("list events")
    }

    fn runtime_with(store: &PgSessionStore, backend: Arc<MockBackend>) -> SessionRuntime {
        SessionRuntime::new(
            store.clone(),
            SandboxRuntime::backend(backend, SandboxSpec::new("mock")),
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn execute_persists_and_flushes_input_through_the_delivery_ledger() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:ledger-execute-{}", Uuid::new_v4())).unwrap();
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
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_status("sbx-existing", SandboxStatus::Running);
        backend.set_observed_resource_uid("sbx-existing", "uid-existing");
        let (io, _stdout, stdin) = mock_io();
        backend.push_io(io).await;
        let runtime = runtime_with(&store, backend.clone());
        let execution = runtime
            .execute_session(
                &thread_key,
                ExecuteSessionInput {
                    idempotency_key: Some("ledger-execute".to_owned()),
                    metadata: None,
                    input_lines: vec![json!({"type": "user", "message": "hello"}).to_string()],
                    idle_timeout_ms: None,
                    max_duration_ms: None,
                },
            )
            .await
            .unwrap();
        let mut stdin = BufReader::new(stdin);
        let mut line = String::new();
        stdin.read_line(&mut line).await.unwrap();
        assert!(line.contains("hello"));
        assert!(
            store
                .list_unresolved_input_deliveries()
                .await
                .unwrap()
                .iter()
                .all(|delivery| delivery.execution_id != execution.execution_id)
        );
        assert!(
            events(&store, &thread_key)
                .await
                .iter()
                .any(|event| event.event_type == "session.input_flushed")
        );
        assert!(
            store
                .terminalize_execution_and_append_event_if_stdout_owner(
                    &execution.execution_id,
                    &runtime.stdout_owner_id,
                    OwnedTerminalEvent::Completed { payload: json!({}) },
                )
                .await
                .unwrap()
                .is_some()
        );
        stop_terminal_stdout_owner_renewer(&runtime.context(), &execution.execution_id).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recovered_input_delivery_arms_the_persisted_max_duration_deadline() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:ledger-recovery-deadline-{}", Uuid::new_v4())).unwrap();
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

        // Construct the durable row as if the prior control plane crashed
        // after persisting it but before it could write stdin or spawn the
        // deadline task.
        let capabilities = default_capabilities();
        let mut metadata = execution_metadata(None, None, Some(350));
        merge_json_object(
            &mut metadata,
            metadata_trace_execution_boundary(&capabilities),
        );
        let prepared = PreparedInputDelivery {
            idempotency_key: "recover-deadline-input".to_owned(),
            message_ids: Vec::new(),
            input_lines: vec![json!({"type": "user", "message": "recover deadline"}).to_string()],
            boundary_fingerprint: input_delivery_boundary_fingerprint(
                &thread_key,
                Some(&metadata),
                &capabilities,
            ),
        };
        let created = store
            .create_execution_with_initial_input_delivery(
                &thread_key,
                "recover-deadline-execution",
                metadata,
                &prepared,
            )
            .await
            .expect("persist delivery before recovery");

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let (io, _stdout, stdin) = mock_io();
        backend.push_io(io).await;
        let runtime = runtime_with(&store, backend);

        runtime.adopt_orphaned_executions().await;

        let mut stdin = BufReader::new(stdin);
        let mut line = String::new();
        timeout(Duration::from_secs(2), stdin.read_line(&mut line))
            .await
            .expect("recovery must flush the durable input")
            .expect("read recovered input");
        assert!(line.contains("recover deadline"));

        // The recovery driver owns the stdout lease, so the later active-row
        // pass skips it. The timer must already have been armed by delivery
        // recovery; otherwise this wait would run until another owner change.
        timeout(
            Duration::from_secs(3),
            wait_for_execution_event(
                &store,
                &thread_key,
                &created.execution.execution_id,
                "session.execution_failed",
            ),
        )
        .await
        .expect("recovered execution must honor its original max duration");
        let failed = store
            .list_events_after(&thread_key, 0, Some(&created.execution.execution_id), 100)
            .await
            .expect("list recovered execution events")
            .into_iter()
            .find(|event| event.event_type == "session.execution_failed")
            .expect("max-duration failure event");
        assert_eq!(failed.payload["reason"], "max_duration_exceeded");
        assert_eq!(failed.payload["max_duration_ms"], 350);
        assert_eq!(
            failed.payload["error"],
            "execution exceeded max_duration_ms=350"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn execution_scoped_event_stream_completes_after_terminal_event() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:stream-close-{}", uuid::Uuid::new_v4())).unwrap();
        let execution_id = orphaned_execution(&store, &thread_key, None, false).await;
        store
            .append_event(
                &thread_key,
                Some(&execution_id),
                "session.output.line",
                json!({ "line": "working" }),
            )
            .await
            .expect("append output event");
        store
            .append_event(
                &thread_key,
                Some(&execution_id),
                "session.execution_completed",
                json!({ "execution_id": execution_id }),
            )
            .await
            .expect("append terminal event");

        // Execution-scoped: the stream must end on its own after emitting the
        // terminal event, releasing the response and its listener connection.
        let listener = store.listen_session_events().await.expect("listener");
        let scoped = session_event_stream(
            store.clone(),
            thread_key.clone(),
            0,
            Some(execution_id.clone()),
            listener,
            tracing::Span::none(),
        );
        let emitted = tokio::time::timeout(Duration::from_secs(10), scoped.collect::<Vec<_>>())
            .await
            .expect("execution-scoped stream should complete after the terminal event");
        let kinds: Vec<_> = emitted
            .into_iter()
            .map(|result| result.expect("stream event").event_type)
            .collect();
        assert_eq!(
            kinds,
            vec!["session.output.line", "session.execution_completed"]
        );

        // Control: an unscoped stream over the same events stays open for
        // future events instead of completing.
        let listener = store.listen_session_events().await.expect("listener");
        let unscoped = session_event_stream(
            store.clone(),
            thread_key.clone(),
            0,
            None,
            listener,
            tracing::Span::none(),
        );
        let mut unscoped = std::pin::pin!(unscoped);
        for _ in 0..2 {
            unscoped
                .next()
                .await
                .expect("buffered event")
                .expect("stream event");
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(300), unscoped.next())
                .await
                .is_err(),
            "unscoped stream should stay open after a terminal event"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn append_messages_generates_missing_session_title_once() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key = ThreadKey::parse(format!("test:title-{}", uuid::Uuid::new_v4())).unwrap();
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

        let calls = Arc::new(AtomicUsize::new(0));
        let sources = Arc::new(Mutex::new(Vec::<String>::new()));
        let generator_started = Arc::new(tokio::sync::Notify::new());
        let generator_release = Arc::new(tokio::sync::Notify::new());
        let calls_for_generator = calls.clone();
        let sources_for_generator = sources.clone();
        let started_for_generator = generator_started.clone();
        let release_for_generator = generator_release.clone();
        let runtime = runtime_with(
            &store,
            Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new())),
        )
        .with_session_title_generator(move |source| {
            let calls = calls_for_generator.clone();
            let sources = sources_for_generator.clone();
            let started = started_for_generator.clone();
            let release = release_for_generator.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                sources.lock().await.push(source);
                started.notify_one();
                release.notified().await;
                Ok("Fix worker memory leak".to_owned())
            }
        });

        tokio::time::timeout(
            Duration::from_secs(1),
            runtime.append_messages(
                &thread_key,
                &[SessionMessageInput {
                    client_message_id: Some("first".to_owned()),
                    role: MessageRole::User,
                    parts: vec![
                        json!({
                            "type": "text",
                            "text": "# Requester Context\n\nThe Slack user who prompted this turn is Alice."
                        }),
                        json!({
                            "type": "text",
                            "text": "<@U123> please fix the memory leak in the worker"
                        }),
                    ],
                    metadata: json!({}),
                }],
            ),
        )
        .await
        .expect("append first message should not wait for title generation")
        .expect("append first message");

        generator_started.notified().await;

        let session = store.get_session(&thread_key).await.unwrap();
        assert_eq!(session.title, None);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            sources.lock().await.clone(),
            vec!["please fix the memory leak in the worker".to_owned()]
        );

        runtime
            .append_messages(
                &thread_key,
                &[SessionMessageInput {
                    client_message_id: Some("burst".to_owned()),
                    role: MessageRole::User,
                    parts: vec![json!({"type": "text", "text": "add more logging"})],
                    metadata: json!({}),
                }],
            )
            .await
            .expect("append burst message");

        assert_eq!(calls.load(Ordering::SeqCst), 1);

        generator_release.notify_one();
        wait_for_session_title(&store, &thread_key, "Fix worker memory leak").await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        runtime
            .append_messages(
                &thread_key,
                &[SessionMessageInput {
                    client_message_id: Some("second".to_owned()),
                    role: MessageRole::User,
                    parts: vec![json!({"type": "text", "text": "add more logging"})],
                    metadata: json!({}),
                }],
            )
            .await
            .expect("append second message");

        let session = store.get_session(&thread_key).await.unwrap();
        assert_eq!(session.title.as_deref(), Some("Fix worker memory leak"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    fn env_value<'a>(spec: &'a SandboxSpec, name: &str) -> Option<&'a str> {
        spec.env
            .iter()
            .find(|env| env.name == name)
            .map(|env| env.value.as_str())
    }

    fn default_capabilities() -> SessionSandboxCapabilities {
        SessionSandboxCapabilities::default_enabled()
    }

    async fn assign_sandbox_identity(
        store: &PgSessionStore,
        thread_key: &ThreadKey,
        sandbox_id: &str,
        resource_uid: &str,
    ) {
        store
            .update_sandbox_assignment(
                thread_key,
                sandbox_id,
                Some(resource_uid),
                &default_capabilities(),
            )
            .await
            .expect("assign fenced sandbox identity");
    }

    fn restricted_capabilities() -> SessionSandboxCapabilities {
        SessionSandboxCapabilities {
            repo_cache: SessionRepoCacheAccess::None,
            observability_enabled: false,
            api_server_enabled: false,
            metadata_trace_enabled: false,
            metadata_trace_expires_at: None,
            metadata_trace_subject_hash: None,
            metadata_trace_consent_revision: None,
            metadata_trace_config_fingerprint: None,
            metadata_trace_config_generation: None,
        }
    }

    fn traced_capabilities() -> SessionSandboxCapabilities {
        SessionSandboxCapabilities {
            metadata_trace_enabled: true,
            metadata_trace_expires_at: Some(OffsetDateTime::now_utc() + TimeDuration::hours(1)),
            metadata_trace_subject_hash: Some("test-trace-subject".to_owned()),
            metadata_trace_consent_revision: Some(1),
            metadata_trace_config_fingerprint: Some("test-trace-config".to_owned()),
            metadata_trace_config_generation: Some(1),
            ..restricted_capabilities()
        }
    }

    fn runtime_with_warm_pool(
        store: &PgSessionStore,
        backend: Arc<MockBackend>,
        workload_marker: impl Into<String>,
    ) -> SessionRuntime {
        let workload_marker = Arc::new(workload_marker.into());
        let claimed_marker = workload_marker.clone();
        let warm_marker = workload_marker.clone();
        let mut runtime = SessionRuntime::new(
            store.clone(),
            SandboxRuntime::backend_with_warm_spec_factory(
                backend,
                move |_thread_key, _execution_id, _harness, _persona| {
                    SandboxSpec::new("mock")
                        .mount(Mount::new(
                            centaur_sandbox_core::MountKind::Bind {
                                source_path: "/var/lib/centaur/repos".to_owned(),
                            },
                            SANDBOX_REPOS_MOUNT_PATH,
                        ))
                        .env("WARM_POOL_TEST_MARKER", claimed_marker.as_str())
                },
                move || SandboxSpec::new("mock").env("WARM_POOL_TEST_MARKER", warm_marker.as_str()),
            ),
        );
        let warm_pool = Arc::new(WarmPoolManager::new(
            runtime.sandbox_runtime.manager.clone(),
            store.clone(),
            runtime.sandbox_runtime.warm_spec_factory.clone().unwrap(),
            runtime.sandbox_runtime.workload_key.clone().unwrap(),
            WarmPoolConfig {
                target_size: 1,
                replenish_interval: Duration::from_secs(60),
                bootstrap_iron_control_principal: None,
                max_running_sandboxes: None,
            },
        ));
        runtime.warm_pool = Some(warm_pool);
        runtime
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_creator_cleanup_never_stops_a_same_name_replacement() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_resource_uid("mock-sbx", "uid-new-owner");
        let runtime = runtime_with(&store, backend.clone());
        let stale_handle = SandboxHandle::new(SandboxId::new("mock-sbx"), "mock")
            .with_resource_uid(Some("uid-stale-creator".to_owned()));

        runtime.stop_created_sandbox_exact(&stale_handle).await;

        assert!(backend.stopped().is_empty());
        assert_eq!(
            backend.status(&SandboxId::new("mock-sbx")).await.unwrap(),
            SandboxStatus::Running
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn capability_mismatch_replaces_existing_sandbox() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:cap-replace-{}", uuid::Uuid::new_v4())).unwrap();
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
            .update_sandbox_assignment(
                &thread_key,
                "sbx-full",
                Some("uid-full"),
                &default_capabilities(),
            )
            .await
            .expect("assign default sandbox");
        let session = store.get_session(&thread_key).await.unwrap();
        let execution_id = store
            .create_execution(&thread_key, None, json!({}))
            .await
            .expect("create execution")
            .execution
            .execution_id;

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_resource_uid("sbx-full", "uid-full");
        let runtime = runtime_with_warm_pool(&store, backend.clone(), thread_key.as_str());
        let sandbox_id = runtime
            .ensure_session_sandbox(EnsureSessionSandboxRequest {
                thread_key: &thread_key,
                harness_type: &HarnessType::Codex,
                persona_id: None,
                existing_sandbox_id: session.sandbox_id.as_deref(),
                existing_sandbox_capabilities: session.sandbox_capabilities.as_ref(),
                iron_control_principal: None,
                proxy_labels: &BTreeMap::new(),
                desired_capabilities: &restricted_capabilities(),
                execution_metadata: None,
                execution_id: &execution_id,
            })
            .await
            .expect("replace sandbox");

        assert_eq!(sandbox_id, "mock-sbx");
        assert_eq!(backend.stopped(), vec!["sbx-full".to_owned()]);
        let session = store.get_session(&thread_key).await.unwrap();
        assert_eq!(session.sandbox_id.as_deref(), Some("mock-sbx"));
        assert_eq!(
            session.sandbox_capabilities,
            Some(restricted_capabilities())
        );
        let spec = backend.created_specs().pop().expect("created cold spec");
        assert!(!spec.capabilities.repo_cache.enabled());
        assert!(!spec.capabilities.observability_enabled);
        assert!(!spec.capabilities.api_server_enabled);
        assert_eq!(
            env_value(&spec, "CENTAUR_SANDBOX_OBSERVABILITY_ENABLED"),
            Some("false")
        );
        assert_eq!(
            env_value(&spec, "CENTAUR_SANDBOX_API_SERVER_ENABLED"),
            Some("false")
        );
        let blocklist = env_value(&spec, "TOOL_BLOCKLIST").unwrap_or("");
        for tool in OBSERVABILITY_TOOL_BLOCKLIST.split(',') {
            assert!(blocklist.split(',').any(|blocked| blocked == tool));
        }
        assert!(
            !spec
                .mounts
                .iter()
                .any(|mount| mount.target_path == SANDBOX_REPOS_MOUNT_PATH)
        );
        let all = events(&store, &thread_key).await;
        assert!(
            all.iter()
                .any(|event| event.event_type == "session.sandbox_capabilities_replaced")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn active_principal_sandbox_reconciliation_stops_revoked_capabilities() {
        let Some(store) = test_store().await else {
            return;
        };
        let _serial = TEST_LOCK.lock().await;
        let thread_key =
            ThreadKey::parse(format!("test:cap-reconcile-{}", uuid::Uuid::new_v4())).unwrap();
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
            .update_sandbox_assignment(
                &thread_key,
                "sbx-revoked",
                Some("uid-revoked"),
                &traced_capabilities(),
            )
            .await
            .expect("assign traced sandbox");
        store
            .set_iron_control_principal(&thread_key, Some("prn-revoked"))
            .await
            .expect("bind principal");

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_resource_uid("sbx-revoked", "uid-revoked");
        let runtime = runtime_with(&store, backend.clone());
        assert_eq!(
            runtime
                .reconcile_active_sandbox_capabilities()
                .await
                .expect("reconcile"),
            1
        );
        assert_eq!(backend.stopped(), vec!["sbx-revoked".to_owned()]);
        assert!(
            store
                .get_session(&thread_key)
                .await
                .unwrap()
                .sandbox_id
                .is_none()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn differently_authenticated_actor_cannot_interrupt_a_traced_sandbox() {
        let Some(store) = test_store().await else {
            return;
        };
        let _serial = TEST_LOCK.lock().await;
        let thread_key =
            ThreadKey::parse(format!("test:trace-interrupt-{}", uuid::Uuid::new_v4())).unwrap();
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
            generation: OffsetDateTime::now_utc().unix_timestamp_nanos() as i64,
            fingerprint: format!("trace-interrupt-{}", uuid::Uuid::new_v4()),
            enabled: true,
        };
        store
            .activate_metadata_trace_config(&identity)
            .await
            .unwrap();
        let expiry = OffsetDateTime::now_utc() + TimeDuration::hours(1);
        let consent = store
            .grant_metadata_trace_consent("slack", "T-interrupt", "U1", expiry)
            .await
            .unwrap();
        let capabilities = SessionSandboxCapabilities {
            metadata_trace_enabled: true,
            metadata_trace_expires_at: Some(expiry),
            metadata_trace_subject_hash: Some("u1".to_owned()),
            metadata_trace_consent_revision: Some(consent.revision),
            metadata_trace_config_fingerprint: Some(identity.fingerprint.clone()),
            metadata_trace_config_generation: Some(identity.generation),
            ..SessionSandboxCapabilities::default_enabled()
        };
        assert!(
            store
                .update_sandbox_assignment_if_metadata_trace_config_active(
                    &thread_key,
                    "sbx-u1",
                    &capabilities,
                    &identity,
                    &SandboxAssignmentSnapshot::unassigned(),
                    "T-interrupt",
                    "U1",
                    "uid-u1",
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
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_resource_uid("sbx-u1", "uid-u1");
        let (io, _stdout, mut stdin) = mock_io();
        backend.push_io(io).await;
        let runtime =
            runtime_with(&store, backend.clone()).with_metadata_trace_config(Some(identity));
        runtime
            .ensure_session_pipe(&thread_key, "sbx-u1")
            .await
            .unwrap();

        let result = runtime
            .interrupt_active_execution(
                &thread_key,
                "U2 identifying reason must not be written",
                Some(&json!({ "slack_actor_team_id": "T-interrupt", "slack_actor_user_id": "U2" })),
            )
            .await;
        assert!(matches!(
            result,
            Err(SessionRuntimeError::MetadataTraceBoundaryChanged)
        ));
        assert_eq!(backend.stopped(), vec!["sbx-u1".to_owned()]);
        let mut bytes = [0; 1];
        assert!(
            tokio::time::timeout(Duration::from_millis(50), stdin.read(&mut bytes))
                .await
                .is_err()
        );
        assert!(
            store
                .get_session(&thread_key)
                .await
                .unwrap()
                .sandbox_id
                .is_none()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unconsenting_actor_replaces_traced_execution_without_tracing_their_message() {
        let Some(store) = test_store().await else {
            return;
        };
        let _serial = TEST_LOCK.lock().await;
        let thread_key =
            ThreadKey::parse(format!("test:trace-replace-{}", uuid::Uuid::new_v4())).unwrap();
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
            generation: OffsetDateTime::now_utc().unix_timestamp_nanos() as i64,
            fingerprint: format!("trace-replace-{}", uuid::Uuid::new_v4()),
            enabled: true,
        };
        store
            .activate_metadata_trace_config(&identity)
            .await
            .unwrap();
        let expiry = OffsetDateTime::now_utc() + TimeDuration::hours(1);
        let consent = store
            .grant_metadata_trace_consent("slack", "T-replace", "U1", expiry)
            .await
            .unwrap();
        let capabilities = SessionSandboxCapabilities {
            metadata_trace_enabled: true,
            metadata_trace_expires_at: Some(expiry),
            metadata_trace_subject_hash: Some("u1".to_owned()),
            metadata_trace_consent_revision: Some(consent.revision),
            metadata_trace_config_fingerprint: Some(identity.fingerprint.clone()),
            metadata_trace_config_generation: Some(identity.generation),
            ..SessionSandboxCapabilities::default_enabled()
        };
        assert!(
            store
                .update_sandbox_assignment_if_metadata_trace_config_active(
                    &thread_key,
                    "sbx-u1",
                    &capabilities,
                    &identity,
                    &SandboxAssignmentSnapshot::unassigned(),
                    "T-replace",
                    "U1",
                    "uid-u1",
                )
                .await
                .unwrap()
        );
        let old_execution = store
            .create_execution(&thread_key, None, json!({}))
            .await
            .unwrap();
        store
            .mark_execution_running(&old_execution.execution.execution_id)
            .await
            .unwrap();
        store
            .merge_execution_metadata(
                &old_execution.execution.execution_id,
                json!({ "metadata_trace_subject_hash": "u1" }),
            )
            .await
            .unwrap();

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_resource_uid("sbx-u1", "uid-u1");
        let (old_io, _old_stdout, _old_stdin) = mock_io();
        let (replacement_io, _replacement_stdout, mut replacement_stdin) = mock_io();
        backend.push_io(old_io).await;
        backend.push_io(replacement_io).await;
        let runtime =
            runtime_with(&store, backend.clone()).with_metadata_trace_config(Some(identity));
        runtime
            .claim_stdout_owner(&old_execution.execution.execution_id)
            .await
            .unwrap();
        runtime
            .ensure_session_pipe(&thread_key, "sbx-u1")
            .await
            .unwrap();

        runtime
            .append_messages(
                &thread_key,
                &[SessionMessageInput {
                    client_message_id: Some("u2-message".to_owned()),
                    role: MessageRole::User,
                    parts: vec![json!({ "type": "text", "text": "U2 must run untraced" })],
                    metadata: json!({
                        "slack_actor_team_id": "T-replace",
                        "slack_actor_user_id": "U2",
                    }),
                }],
            )
            .await
            .unwrap();

        let mut bytes = [0; 4096];
        let read = tokio::time::timeout(Duration::from_secs(1), replacement_stdin.read(&mut bytes))
            .await
            .expect("replacement receives the durable U2 message")
            .unwrap();
        assert!(
            std::str::from_utf8(&bytes[..read])
                .unwrap()
                .contains("U2 must run untraced")
        );
        assert_eq!(backend.stopped(), vec!["sbx-u1".to_owned()]);
        assert!(
            !backend.created_specs()[0]
                .capabilities
                .metadata_trace_enabled,
            "the replacement sandbox must not create U2 OTLP trace spans"
        );
        let all_events = events(&store, &thread_key).await;
        assert!(all_events.iter().any(|event| {
            event.event_type == "session.steering_replaced_trace_boundary"
                && event.execution_id.as_deref()
                    != Some(old_execution.execution.execution_id.as_str())
        }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn revoke_acknowledges_after_input_fence_before_stalled_exact_drain() {
        let Some(store) = test_store().await else {
            return;
        };
        let _serial = TEST_LOCK.lock().await;
        let thread_key =
            ThreadKey::parse(format!("test:trace-stall-{}", uuid::Uuid::new_v4())).unwrap();
        let workspace_id = format!("T-stall-{}", uuid::Uuid::new_v4());
        let user_id = format!("U-stall-{}", uuid::Uuid::new_v4());
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
            generation: OffsetDateTime::now_utc().unix_timestamp_nanos() as i64,
            fingerprint: format!("trace-stall-{}", uuid::Uuid::new_v4()),
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
        let capabilities = SessionSandboxCapabilities {
            metadata_trace_enabled: true,
            metadata_trace_expires_at: Some(expiry),
            metadata_trace_subject_hash: Some("u1".to_owned()),
            metadata_trace_consent_revision: Some(consent.revision),
            metadata_trace_config_fingerprint: Some(identity.fingerprint.clone()),
            metadata_trace_config_generation: Some(identity.generation),
            ..SessionSandboxCapabilities::default_enabled()
        };
        assert!(
            store
                .update_sandbox_assignment_if_metadata_trace_config_active(
                    &thread_key,
                    "sbx-stall",
                    &capabilities,
                    &identity,
                    &SandboxAssignmentSnapshot::unassigned(),
                    &workspace_id,
                    &user_id,
                    "uid-stall"
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
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_resource_uid("sbx-stall", "uid-stall");
        backend.set_stop_delay(Some(Duration::from_secs(10)));
        let (io, _stdout, _stdin) = mock_io();
        backend.push_io(io).await;
        let runtime =
            runtime_with(&store, backend.clone()).with_metadata_trace_config(Some(identity));
        let pipe = runtime
            .ensure_session_pipe(&thread_key, "sbx-stall")
            .await
            .unwrap();
        assert!(
            !pipe
                .trace_assignment_epoch
                .as_deref()
                .unwrap_or_default()
                .is_empty()
        );
        assert_eq!(pipe.trace_resource_uid.as_deref(), Some("uid-stall"));
        METADATA_TRACE_INPUT_TEST_TIMEOUT_MS.store(20, Ordering::Relaxed);
        let write_runtime = runtime.clone();
        let write_capabilities = capabilities.clone();
        let write_thread_key = thread_key.clone();
        let write_execution_id = execution.execution.execution_id.clone();
        let writer = tokio::spawn(async move {
            write_runtime
                .write_traced_input_lines(
                    &pipe,
                    &write_thread_key,
                    &write_execution_id,
                    "sbx-stall",
                    &write_capabilities,
                    &["x".repeat(128 * 1024)],
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(2)).await;
        let revoke_runtime = runtime.clone();
        let revoke_workspace_id = workspace_id.clone();
        let revoke_user_id = user_id.clone();
        let revoke = tokio::time::timeout(Duration::from_secs(1), async move {
            revoke_runtime
                .revoke_slack_trace_consent(&revoke_workspace_id, &revoke_user_id, None)
                .await
        })
        .await;
        METADATA_TRACE_INPUT_TEST_TIMEOUT_MS.store(30_000, Ordering::Relaxed);
        let revoked = revoke
            .expect("revoke must not wait for a stalled exact stop")
            .expect("revoke succeeds");
        assert!(!revoked.enabled);
        assert!(revoked.drain_pending);
        assert!(
            backend.stopped().is_empty(),
            "acknowledgement must not stop"
        );
        assert!(matches!(
            writer.await.unwrap(),
            Err(SessionRuntimeError::MetadataTraceBoundaryChanged)
        ));
        assert!(
            store
                .lock_metadata_trace_input(
                    &capabilities,
                    &thread_key,
                    &execution.execution.execution_id,
                    "sbx-stall",
                    "epoch-stall",
                    "uid-stall",
                )
                .await
                .unwrap()
                .is_none(),
            "the committed acknowledgement must fence all later traced input"
        );

        backend.set_stop_delay(None);
        runtime
            .reconcile_active_sandbox_capabilities()
            .await
            .expect("reconcile pending drain");
        assert_eq!(backend.stopped(), vec!["sbx-stall".to_owned()]);
        assert!(
            store
                .get_session(&thread_key)
                .await
                .unwrap()
                .sandbox_id
                .is_none(),
            "reconciliation retires the exact durable assignment"
        );
        assert!(
            !store
                .metadata_trace_consent("slack", &workspace_id, &user_id)
                .await
                .unwrap()
                .drain_pending,
            "a completed exact drain releases the durable grant fence"
        );
        let regranted = store
            .grant_metadata_trace_consent(
                "slack",
                &workspace_id,
                &user_id,
                OffsetDateTime::now_utc() + TimeDuration::hours(1),
            )
            .await
            .unwrap();
        runtime
            .drain_slack_trace_consent(&revoked)
            .await
            .expect("a stale replica drain is a no-op");
        assert_eq!(
            store
                .metadata_trace_consent("slack", &workspace_id, &user_id)
                .await
                .unwrap(),
            regranted,
            "a stale reconciler must not revoke a later grant"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transient_drain_observation_error_keeps_the_exact_assignment_pending() {
        let Some(store) = test_store().await else {
            return;
        };
        let _serial = TEST_LOCK.lock().await;
        let thread_key =
            ThreadKey::parse(format!("test:trace-observe-{}", uuid::Uuid::new_v4())).unwrap();
        let workspace_id = format!("T-observe-{}", uuid::Uuid::new_v4());
        let user_id = format!("U-observe-{}", uuid::Uuid::new_v4());
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
            generation: OffsetDateTime::now_utc().unix_timestamp_nanos() as i64,
            fingerprint: format!("trace-observe-{}", uuid::Uuid::new_v4()),
            enabled: true,
        };
        store
            .activate_metadata_trace_config(&identity)
            .await
            .unwrap();
        let expiry = OffsetDateTime::now_utc() + TimeDuration::hours(1);
        let granted = store
            .grant_metadata_trace_consent("slack", &workspace_id, &user_id, expiry)
            .await
            .unwrap();
        let capabilities = SessionSandboxCapabilities {
            metadata_trace_enabled: true,
            metadata_trace_expires_at: Some(expiry),
            metadata_trace_subject_hash: Some("observe-subject".to_owned()),
            metadata_trace_consent_revision: Some(granted.revision),
            metadata_trace_config_fingerprint: Some(identity.fingerprint.clone()),
            metadata_trace_config_generation: Some(identity.generation),
            ..SessionSandboxCapabilities::default_enabled()
        };
        assert!(
            store
                .update_sandbox_assignment_if_metadata_trace_config_active(
                    &thread_key,
                    "sbx-observe",
                    &capabilities,
                    &identity,
                    &SandboxAssignmentSnapshot::unassigned(),
                    &workspace_id,
                    &user_id,
                    "uid-observe",
                )
                .await
                .unwrap()
        );
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_resource_uid("sbx-observe", "uid-observe");
        let runtime =
            runtime_with(&store, backend.clone()).with_metadata_trace_config(Some(identity));
        let revoked = runtime
            .revoke_slack_trace_consent(&workspace_id, &user_id, None)
            .await
            .unwrap();
        let targets = store
            .metadata_trace_drain_targets_if_current(
                "slack",
                &workspace_id,
                &user_id,
                revoked.revision,
            )
            .await
            .unwrap()
            .unwrap();
        let [target] = targets.as_slice() else {
            panic!("revoke must retain exactly one trace assignment drain target");
        };
        let assignment_lock = store
            .lock_sandbox_assignment_for_reconciliation(&thread_key, &target.sandbox_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            assignment_lock.resource_uid(),
            Some(target.resource_uid.as_str())
        );
        assert_eq!(
            assignment_lock.metadata_trace_assignment_epoch(),
            Some(target.assignment_epoch.as_str()),
            "the lock must validate the trace epoch recorded by the revoke target"
        );
        assignment_lock.rollback().await.unwrap();

        backend.fail_observe();
        runtime.drain_slack_trace_consent(&revoked).await.unwrap();
        assert!(backend.stopped().is_empty());
        assert_eq!(
            store
                .get_session(&thread_key)
                .await
                .unwrap()
                .sandbox_id
                .as_deref(),
            Some("sbx-observe"),
            "a transient observation error must not clear a live assignment"
        );
        assert!(
            store
                .metadata_trace_consent("slack", &workspace_id, &user_id)
                .await
                .unwrap()
                .drain_pending,
            "a transient observation error must remain retryable"
        );

        backend.allow_observe();
        backend.clear_observed_resource_uid("sbx-observe");
        backend.fail_stop();
        runtime.drain_slack_trace_consent(&revoked).await.unwrap();
        assert!(backend.stopped().is_empty());
        assert_eq!(
            store
                .get_session(&thread_key)
                .await
                .unwrap()
                .sandbox_id
                .as_deref(),
            Some("sbx-observe"),
            "an observation without a UID must not clear the durable target"
        );
        assert!(
            store
                .metadata_trace_consent("slack", &workspace_id, &user_id)
                .await
                .unwrap()
                .drain_pending,
            "a missing observation UID must remain retryable"
        );

        backend.allow_stop();
        backend.set_observed_resource_uid("sbx-observe", "uid-observe");
        backend.preserve_status_after_stop(true);
        runtime.drain_slack_trace_consent(&revoked).await.unwrap();
        assert_eq!(backend.stopped(), vec!["sbx-observe".to_owned()]);
        assert_eq!(
            store
                .get_session(&thread_key)
                .await
                .unwrap()
                .sandbox_id
                .as_deref(),
            Some("sbx-observe"),
            "a successful stop without Gone or a concrete replacement UID is not a drain proof"
        );
        assert!(
            store
                .metadata_trace_consent("slack", &workspace_id, &user_id)
                .await
                .unwrap()
                .drain_pending,
            "post-stop observation without a UID must remain retryable"
        );

        backend.preserve_status_after_stop(false);
        backend.set_observed_resource_uid("sbx-observe", "uid-observe");
        runtime.drain_slack_trace_consent(&revoked).await.unwrap();
        assert_eq!(
            backend.stopped(),
            vec!["sbx-observe".to_owned(), "sbx-observe".to_owned()]
        );
        assert!(
            store
                .get_session(&thread_key)
                .await
                .unwrap()
                .sandbox_id
                .is_none()
        );
        assert!(
            !store
                .metadata_trace_consent("slack", &workspace_id, &user_id)
                .await
                .unwrap()
                .drain_pending
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn active_principal_reconciliation_preserves_a_concurrent_replacement() {
        // Unlike the opportunistic adoption coverage, this regression must run
        // against Postgres in CI: its assertion is the conditional database
        // update that fences a replacement committed after the sweep snapshot.
        let Some(store) = test_store().await else {
            return;
        };
        let _serial = TEST_LOCK.lock().await;
        let thread_key = ThreadKey::parse(format!(
            "test:cap-reconcile-replacement-{}",
            uuid::Uuid::new_v4()
        ))
        .unwrap();
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
            .update_sandbox_assignment(
                &thread_key,
                "sbx-observed",
                None,
                &restricted_capabilities(),
            )
            .await
            .expect("assign observed sandbox");
        store
            .set_iron_control_principal(&thread_key, Some("prn-revoked"))
            .await
            .expect("bind principal");
        let observed = store
            .get_session(&thread_key)
            .await
            .expect("read sweep snapshot");

        // A resumed execution may replace the sandbox after the reconciler
        // listed this session but before its stop operation. The old snapshot
        // must never clear the new canonical assignment.
        store
            .update_sandbox_assignment(
                &thread_key,
                "sbx-replacement",
                None,
                &default_capabilities(),
            )
            .await
            .expect("replace sandbox concurrently");

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let runtime = runtime_with(&store, backend.clone());
        assert!(
            !runtime
                .reconcile_session_sandbox_capabilities(&observed, default_capabilities())
                .await
                .expect("reconcile stale snapshot")
        );
        assert!(backend.stopped().is_empty());
        assert_eq!(
            store
                .get_session(&thread_key)
                .await
                .expect("read replacement")
                .sandbox_id
                .as_deref(),
            Some("sbx-replacement")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_capability_stop_keeps_assignment_for_retry() {
        let Some(store) = test_store().await else {
            return;
        };
        let _serial = TEST_LOCK.lock().await;
        let thread_key =
            ThreadKey::parse(format!("test:cap-stop-retry-{}", uuid::Uuid::new_v4())).unwrap();
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
            .update_sandbox_assignment(
                &thread_key,
                "sbx-stop-retry",
                Some("uid-stop-retry"),
                &restricted_capabilities(),
            )
            .await
            .expect("assign sandbox");
        let observed = store.get_session(&thread_key).await.expect("read session");

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_resource_uid("sbx-stop-retry", "uid-stop-retry");
        backend.fail_stop();
        let runtime = runtime_with(&store, backend.clone());
        assert!(
            runtime
                .reconcile_session_sandbox_capabilities(&observed, default_capabilities())
                .await
                .is_err()
        );
        assert_eq!(
            store
                .get_session(&thread_key)
                .await
                .expect("read retryable assignment")
                .sandbox_id
                .as_deref(),
            Some("sbx-stop-retry")
        );

        backend.allow_stop();
        assert!(
            runtime
                .reconcile_session_sandbox_capabilities(&observed, default_capabilities())
                .await
                .expect("retry stop")
        );
        assert!(
            store
                .get_session(&thread_key)
                .await
                .expect("read cleared assignment")
                .sandbox_id
                .is_none()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn non_default_capabilities_skip_warm_pool() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:cap-warm-skip-{}", uuid::Uuid::new_v4())).unwrap();
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

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let runtime = runtime_with_warm_pool(&store, backend.clone(), thread_key.as_str());
        let workload_key = runtime
            .warm_pool
            .as_ref()
            .unwrap()
            .workload_key()
            .to_owned();
        let warm_sandbox_id = format!("warm-sbx-{}", uuid::Uuid::new_v4());
        store
            .insert_ready_warm_sandbox(&warm_sandbox_id, Some("uid-warm"), &workload_key)
            .await
            .expect("insert warm sandbox");

        let sandbox_id = runtime
            .ensure_session_sandbox(EnsureSessionSandboxRequest {
                thread_key: &thread_key,
                harness_type: &HarnessType::Codex,
                persona_id: None,
                existing_sandbox_id: None,
                existing_sandbox_capabilities: None,
                iron_control_principal: None,
                proxy_labels: &BTreeMap::new(),
                desired_capabilities: &restricted_capabilities(),
                execution_metadata: None,
                execution_id: &execution_id,
            })
            .await
            .expect("ensure sandbox");

        assert_eq!(sandbox_id, "mock-sbx");
        let claimed = store
            .claim_ready_warm_sandbox(&workload_key, thread_key.as_str())
            .await
            .expect("warm row should remain ready")
            .expect("warm sandbox remains claimable");
        assert_eq!(claimed.sandbox_id, warm_sandbox_id);
        let session = store.get_session(&thread_key).await.unwrap();
        assert_eq!(
            session.sandbox_capabilities,
            Some(restricted_capabilities())
        );
        let spec = backend.created_specs().pop().expect("created cold spec");
        assert!(!spec.capabilities.repo_cache.enabled());
        assert!(!spec.capabilities.observability_enabled);
        assert!(!spec.capabilities.api_server_enabled);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn terminal_existing_sandbox_is_retired_before_assignment_replacement() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:terminal-replace-{}", Uuid::new_v4())).unwrap();
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
        assign_sandbox_identity(&store, &thread_key, "sbx-terminating", "uid-terminating").await;
        let session = store.get_session(&thread_key).await.unwrap();
        let execution_id = store
            .create_execution(&thread_key, None, json!({}))
            .await
            .unwrap()
            .execution
            .execution_id;
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_status("sbx-terminating", SandboxStatus::Gone);
        backend.set_observed_resource_uid("sbx-terminating", "uid-terminating");
        let runtime = runtime_with(&store, backend.clone());

        let replacement = runtime
            .ensure_session_sandbox(EnsureSessionSandboxRequest {
                thread_key: &thread_key,
                harness_type: &HarnessType::Codex,
                persona_id: None,
                existing_sandbox_id: session.sandbox_id.as_deref(),
                existing_sandbox_capabilities: session.sandbox_capabilities.as_ref(),
                iron_control_principal: None,
                proxy_labels: &BTreeMap::new(),
                desired_capabilities: &default_capabilities(),
                execution_metadata: None,
                execution_id: &execution_id,
            })
            .await
            .unwrap();

        assert_eq!(replacement, "mock-sbx");
        assert_eq!(backend.stopped(), vec!["sbx-terminating".to_owned()]);
        assert_eq!(
            store
                .get_session(&thread_key)
                .await
                .unwrap()
                .sandbox_id
                .as_deref(),
            Some("mock-sbx")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn existing_running_sandbox_ensures_proxy_before_reuse() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:proxy-reuse-{}", uuid::Uuid::new_v4())).unwrap();
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
        assign_sandbox_identity(&store, &thread_key, "sbx-existing", "uid-existing").await;
        let execution_id = store
            .create_execution(&thread_key, None, json!({}))
            .await
            .expect("create execution")
            .execution
            .execution_id;

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_status("sbx-existing", SandboxStatus::Running);
        backend.set_observed_resource_uid("sbx-existing", "uid-existing");
        let runtime = runtime_with(&store, backend.clone());
        let proxy_labels =
            BTreeMap::from([("centaur.slack_user_id".to_owned(), "U0123456789".to_owned())]);
        let sandbox_id = runtime
            .ensure_session_sandbox(EnsureSessionSandboxRequest {
                thread_key: &thread_key,
                harness_type: &HarnessType::Codex,
                persona_id: None,
                existing_sandbox_id: Some("sbx-existing"),
                existing_sandbox_capabilities: None,
                iron_control_principal: Some("principal-existing"),
                proxy_labels: &proxy_labels,
                desired_capabilities: &SessionSandboxCapabilities::default_enabled(),
                execution_metadata: None,
                execution_id: &execution_id,
            })
            .await
            .expect("reuse existing sandbox");

        assert_eq!(sandbox_id, "sbx-existing");
        assert_eq!(
            backend.proxy_ensures(),
            vec![(
                "sbx-existing".to_owned(),
                "principal-existing".to_owned(),
                proxy_labels
            )]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn running_fence_rejects_same_name_uid_replacement_before_opening_io() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:running-fence-uid-{}", Uuid::new_v4())).unwrap();
        let sandbox_id = format!("sbx-running-fence-{}", Uuid::new_v4());
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
        assign_sandbox_identity(&store, &thread_key, &sandbox_id, "uid-old").await;
        let execution_id = store
            .create_execution(&thread_key, None, json!({}))
            .await
            .unwrap()
            .execution
            .execution_id;
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_status(&sandbox_id, SandboxStatus::Running);
        backend.set_observed_resource_uid(&sandbox_id, "uid-replacement");
        let runtime = runtime_with(&store, backend.clone());

        assert!(matches!(
            runtime
                .ensure_session_sandbox(EnsureSessionSandboxRequest {
                    thread_key: &thread_key,
                    harness_type: &HarnessType::Codex,
                    persona_id: None,
                    existing_sandbox_id: Some(&sandbox_id),
                    existing_sandbox_capabilities: None,
                    iron_control_principal: None,
                    proxy_labels: &BTreeMap::new(),
                    desired_capabilities: &SessionSandboxCapabilities::default_enabled(),
                    execution_metadata: None,
                    execution_id: &execution_id,
                })
                .await,
            Err(SessionRuntimeError::Sandbox(_))
                | Err(SessionRuntimeError::SandboxAssignmentChanged)
        ));
        assert_eq!(backend.opens(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn legacy_non_trace_principal_assignment_is_identity_fenced_and_retired() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:legacy-principal-{}", Uuid::new_v4())).unwrap();
        let sandbox_id = format!("sbx-legacy-principal-{}", Uuid::new_v4());
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
        store
            .update_sandbox_id(&thread_key, Some(&sandbox_id))
            .await
            .unwrap();
        sqlx::query("update sessions set iron_control_principal = $2 where thread_key = $1")
            .bind(thread_key.as_str())
            .bind("principal-revoked")
            .execute(store.pool())
            .await
            .unwrap();

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_status(&sandbox_id, SandboxStatus::Running);
        backend.set_observed_resource_uid(&sandbox_id, "uid-legacy-principal");
        let runtime = runtime_with(&store, backend.clone());
        let session = store.get_session(&thread_key).await.unwrap();
        assert_eq!(
            session.iron_control_principal.as_deref(),
            Some("principal-revoked")
        );

        assert!(
            runtime
                .reconcile_session_sandbox_capabilities(
                    &session,
                    SessionSandboxCapabilities::default_enabled(),
                )
                .await
                .unwrap()
        );
        assert_eq!(backend.stopped(), vec![sandbox_id.clone()]);
        assert!(
            store
                .get_session(&thread_key)
                .await
                .unwrap()
                .sandbox_id
                .is_none()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn partial_legacy_trace_identity_is_adopted_and_retired() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:partial-trace-{}", Uuid::new_v4())).unwrap();
        let sandbox_id = format!("sbx-partial-trace-{}", Uuid::new_v4());
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
        store
            .update_sandbox_id(&thread_key, Some(&sandbox_id))
            .await
            .unwrap();
        sqlx::query(
            r#"
            update sessions
            set sandbox_metadata_trace_assignment_epoch = 'legacy-trace-epoch',
                sandbox_metadata_trace_resource_uid = null,
                sandbox_assignment_epoch = 'legacy-trace-epoch',
                sandbox_resource_uid = null
            where thread_key = $1
            "#,
        )
        .bind(thread_key.as_str())
        .execute(store.pool())
        .await
        .unwrap();

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_status(&sandbox_id, SandboxStatus::Running);
        backend.set_observed_resource_uid(&sandbox_id, "uid-partial-trace");
        let runtime = runtime_with(&store, backend.clone());
        let session = store.get_session(&thread_key).await.unwrap();

        assert!(
            runtime
                .reconcile_session_sandbox_capabilities(
                    &session,
                    SessionSandboxCapabilities::default_enabled(),
                )
                .await
                .unwrap()
        );
        assert_eq!(backend.stopped(), vec![sandbox_id.clone()]);
        assert!(
            store
                .get_session(&thread_key)
                .await
                .unwrap()
                .sandbox_id
                .is_none()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn capability_reconciliation_stop_timeout_releases_assignment_lock() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        ASSIGNMENT_RECONCILIATION_BACKEND_TIMEOUT_MS.store(5, Ordering::SeqCst);
        let thread_key =
            ThreadKey::parse(format!("test:reconcile-stop-timeout-{}", Uuid::new_v4())).unwrap();
        let sandbox_id = format!("sbx-reconcile-stop-timeout-{}", Uuid::new_v4());
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
        assign_sandbox_identity(&store, &thread_key, &sandbox_id, "uid-stop-timeout").await;
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_status(&sandbox_id, SandboxStatus::Running);
        backend.set_observed_resource_uid(&sandbox_id, "uid-stop-timeout");
        backend.set_stop_delay(Some(Duration::from_secs(1)));
        let runtime = runtime_with(&store, backend);
        let session = store.get_session(&thread_key).await.unwrap();

        assert!(matches!(
            runtime
                .reconcile_session_sandbox_capabilities(
                    &session,
                    SessionSandboxCapabilities::default_enabled(),
                )
                .await,
            Err(SessionRuntimeError::Sandbox(_))
        ));
        let assignment_lock = timeout(
            Duration::from_secs(1),
            store.lock_sandbox_assignment_for_reconciliation(&thread_key, &sandbox_id),
        )
        .await
        .expect("timed-out backend stop must release the database row lock")
        .unwrap()
        .expect("assignment remains retryable after timeout");
        assignment_lock.rollback().await.unwrap();
        ASSIGNMENT_RECONCILIATION_BACKEND_TIMEOUT_MS.store(10_000, Ordering::SeqCst);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn legacy_observation_timeout_releases_assignment_lock() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        ASSIGNMENT_RECONCILIATION_BACKEND_TIMEOUT_MS.store(5, Ordering::SeqCst);
        let thread_key =
            ThreadKey::parse(format!("test:legacy-observe-timeout-{}", Uuid::new_v4())).unwrap();
        let sandbox_id = format!("sbx-legacy-observe-timeout-{}", Uuid::new_v4());
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
        store
            .update_sandbox_id(&thread_key, Some(&sandbox_id))
            .await
            .unwrap();
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_status(&sandbox_id, SandboxStatus::Running);
        backend.set_observed_resource_uid(&sandbox_id, "uid-observe-timeout");
        backend.block_next_status();
        let runtime = runtime_with(&store, backend.clone());
        let session = store.get_session(&thread_key).await.unwrap();

        assert!(matches!(
            runtime
                .reconcile_session_sandbox_capabilities(
                    &session,
                    SessionSandboxCapabilities::default_enabled(),
                )
                .await,
            Err(SessionRuntimeError::Sandbox(_))
        ));
        let assignment_lock = timeout(
            Duration::from_secs(1),
            store.lock_sandbox_assignment_for_reconciliation(&thread_key, &sandbox_id),
        )
        .await
        .expect("timed-out backend observation must release the database row lock")
        .unwrap()
        .expect("legacy assignment remains retryable after timeout");
        assignment_lock.rollback().await.unwrap();
        backend.release_status();
        ASSIGNMENT_RECONCILIATION_BACKEND_TIMEOUT_MS.store(10_000, Ordering::SeqCst);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn created_and_suspended_reuse_both_fence_before_opening_io() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        for status in [SandboxStatus::Created, SandboxStatus::Suspended] {
            let thread_key =
                ThreadKey::parse(format!("test:reuse-fence-{}", Uuid::new_v4())).unwrap();
            let sandbox_id = format!("sbx-reuse-fence-{}", Uuid::new_v4());
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
            assign_sandbox_identity(&store, &thread_key, &sandbox_id, "uid-reuse-fence").await;
            let execution_id = store
                .create_execution(&thread_key, None, json!({}))
                .await
                .unwrap()
                .execution
                .execution_id;
            let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
            backend.set_observed_status(&sandbox_id, status);
            backend.set_observed_resource_uid(&sandbox_id, "uid-reuse-fence");
            let runtime = runtime_with(&store, backend.clone());

            runtime
                .ensure_session_sandbox(EnsureSessionSandboxRequest {
                    thread_key: &thread_key,
                    harness_type: &HarnessType::Codex,
                    persona_id: None,
                    existing_sandbox_id: Some(&sandbox_id),
                    existing_sandbox_capabilities: None,
                    iron_control_principal: None,
                    proxy_labels: &BTreeMap::new(),
                    desired_capabilities: &SessionSandboxCapabilities::default_enabled(),
                    execution_metadata: None,
                    execution_id: &execution_id,
                })
                .await
                .unwrap();
            assert_eq!(backend.running_fence_count(), 1);
            assert_eq!(backend.opens(), 0);
            store.complete_execution(&execution_id).await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_reuse_fences_hold_capacity_for_running_and_suspended() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        for first_status in [SandboxStatus::Suspended, SandboxStatus::Running] {
            let backend = Arc::new(MockBackend::new(SandboxStatus::Suspended, Vec::new()));
            let mut sessions = Vec::new();
            for suffix in ["first", "second"] {
                let thread_key =
                    ThreadKey::parse(format!("test:resume-capacity-{suffix}-{}", Uuid::new_v4()))
                        .unwrap();
                let sandbox_id = format!("sbx-resume-capacity-{suffix}-{}", Uuid::new_v4());
                let resource_uid = format!("uid-resume-capacity-{suffix}");
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
                assign_sandbox_identity(&store, &thread_key, &sandbox_id, &resource_uid).await;
                let execution_id = store
                    .create_execution(&thread_key, None, json!({}))
                    .await
                    .unwrap()
                    .execution
                    .execution_id;
                let status = if suffix == "first" {
                    first_status.clone()
                } else {
                    SandboxStatus::Suspended
                };
                backend.set_observed_status(&sandbox_id, status);
                backend.set_observed_resource_uid(&sandbox_id, &resource_uid);
                sessions.push((thread_key, sandbox_id, execution_id));
            }

            let runtime = Arc::new(runtime_with(&store, backend.clone()).with_sandbox_capacity(
                SandboxCapacityConfig {
                    max_running: 1,
                    hot_idle_grace: Duration::from_secs(60),
                },
            ));
            backend.block_next_running_fence();

            let (first_thread, first_sandbox, first_execution) = sessions.remove(0);
            let first_runtime = runtime.clone();
            let first = tokio::spawn(async move {
                first_runtime
                    .ensure_session_sandbox(EnsureSessionSandboxRequest {
                        thread_key: &first_thread,
                        harness_type: &HarnessType::Codex,
                        persona_id: None,
                        existing_sandbox_id: Some(&first_sandbox),
                        existing_sandbox_capabilities: None,
                        iron_control_principal: None,
                        proxy_labels: &BTreeMap::new(),
                        desired_capabilities: &SessionSandboxCapabilities::default_enabled(),
                        execution_metadata: None,
                        execution_id: &first_execution,
                    })
                    .await
            });
            backend.wait_for_running_fence().await;

            let (second_thread, second_sandbox, second_execution) = sessions.remove(0);
            let second_runtime = runtime.clone();
            let mut second = tokio::spawn(async move {
                second_runtime
                    .ensure_session_sandbox(EnsureSessionSandboxRequest {
                        thread_key: &second_thread,
                        harness_type: &HarnessType::Codex,
                        persona_id: None,
                        existing_sandbox_id: Some(&second_sandbox),
                        existing_sandbox_capabilities: None,
                        iron_control_principal: None,
                        proxy_labels: &BTreeMap::new(),
                        desired_capabilities: &SessionSandboxCapabilities::default_enabled(),
                        execution_metadata: None,
                        execution_id: &second_execution,
                    })
                    .await
            });
            assert!(
                timeout(Duration::from_millis(50), &mut second)
                    .await
                    .is_err(),
                "the second resume must wait for the first running fence to release capacity"
            );

            backend.release_running_fence();
            first.await.unwrap().unwrap();
            assert!(matches!(
                second.await.unwrap(),
                Err(SessionRuntimeError::CapacityExceeded { max_running: 1, .. })
            ));
            let running = backend
                .list_observed()
                .await
                .unwrap()
                .into_iter()
                .filter(|sandbox| sandbox.status == SandboxStatus::Running)
                .count();
            assert_eq!(running, 1);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn running_fence_timeout_returns_before_any_io_open() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        ASSIGNMENT_RECONCILIATION_BACKEND_TIMEOUT_MS.store(5, Ordering::SeqCst);
        let thread_key =
            ThreadKey::parse(format!("test:running-fence-timeout-{}", Uuid::new_v4())).unwrap();
        let sandbox_id = format!("sbx-running-fence-timeout-{}", Uuid::new_v4());
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
        assign_sandbox_identity(&store, &thread_key, &sandbox_id, "uid-fence-timeout").await;
        let execution_id = store
            .create_execution(&thread_key, None, json!({}))
            .await
            .unwrap()
            .execution
            .execution_id;
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_status(&sandbox_id, SandboxStatus::Running);
        backend.set_observed_resource_uid(&sandbox_id, "uid-fence-timeout");
        backend.block_next_running_fence();
        let runtime = runtime_with(&store, backend.clone());
        let task = tokio::spawn(async move {
            runtime
                .ensure_session_sandbox(EnsureSessionSandboxRequest {
                    thread_key: &thread_key,
                    harness_type: &HarnessType::Codex,
                    persona_id: None,
                    existing_sandbox_id: Some(&sandbox_id),
                    existing_sandbox_capabilities: None,
                    iron_control_principal: None,
                    proxy_labels: &BTreeMap::new(),
                    desired_capabilities: &SessionSandboxCapabilities::default_enabled(),
                    execution_metadata: None,
                    execution_id: &execution_id,
                })
                .await
        });
        backend.wait_for_running_fence().await;
        assert!(matches!(
            task.await.unwrap(),
            Err(SessionRuntimeError::Sandbox(_))
        ));
        assert_eq!(backend.opens(), 0);
        backend.release_running_fence();
        ASSIGNMENT_RECONCILIATION_BACKEND_TIMEOUT_MS.store(10_000, Ordering::SeqCst);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn public_delivery_fence_timeout_stays_ambiguous_and_writes_zero_bytes() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        ASSIGNMENT_RECONCILIATION_BACKEND_TIMEOUT_MS.store(5, Ordering::SeqCst);
        let thread_key =
            ThreadKey::parse(format!("test:delivery-fence-timeout-{}", Uuid::new_v4())).unwrap();
        let sandbox_id = format!("sbx-delivery-fence-timeout-{}", Uuid::new_v4());
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
        assign_sandbox_identity(&store, &thread_key, &sandbox_id, "uid-delivery-fence").await;
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_status(&sandbox_id, SandboxStatus::Running);
        backend.set_observed_resource_uid(&sandbox_id, "uid-delivery-fence");
        backend.block_next_running_fence();
        let runtime = runtime_with(&store, backend.clone());
        let run_thread = thread_key.clone();
        let task = tokio::spawn(async move {
            runtime
                .execute_session(
                    &run_thread,
                    ExecuteSessionInput {
                        idempotency_key: Some("fence-timeout".to_owned()),
                        metadata: None,
                        input_lines: vec![
                            json!({"type": "user", "message": "do not flush"}).to_string(),
                        ],
                        idle_timeout_ms: None,
                        max_duration_ms: None,
                    },
                )
                .await
        });
        backend.wait_for_running_fence().await;
        assert!(matches!(
            task.await.unwrap(),
            Err(SessionRuntimeError::Sandbox(_))
        ));
        let unresolved = store.list_unresolved_input_deliveries().await.unwrap();
        let delivery = unresolved
            .iter()
            .find(|delivery| delivery.thread_key == thread_key)
            .expect("fence failure keeps durable delivery unresolved");
        assert_eq!(delivery.state, InputDeliveryState::Ambiguous);
        assert_eq!(delivery.input_lines.len(), 1);
        assert!(delivery.input_lines[0].contains("do not flush"));
        assert_eq!(backend.opens(), 0);
        backend.release_running_fence();
        ASSIGNMENT_RECONCILIATION_BACKEND_TIMEOUT_MS.store(10_000, Ordering::SeqCst);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn capacity_pressure_pauses_oldest_idle_assigned_sandbox() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_status(
            "sbx-old",
            SandboxStatus::Unknown("status temporarily unavailable".to_owned()),
        );
        backend.set_observed_status("sbx-hot", SandboxStatus::Running);
        backend.set_observed_status("sbx-stale", SandboxStatus::Gone);
        backend.set_observed_status("sbx-paused", SandboxStatus::Suspended);
        backend.set_observed_resource_uid("sbx-old", "uid-old");
        backend.set_observed_resource_uid("sbx-hot", "uid-hot");
        backend.set_observed_resource_uid("sbx-stale", "uid-stale");
        backend.set_observed_resource_uid("sbx-paused", "uid-paused");

        let stale_thread =
            ThreadKey::parse(format!("test:capacity-stale-{}", uuid::Uuid::new_v4())).unwrap();
        let paused_thread =
            ThreadKey::parse(format!("test:capacity-paused-{}", uuid::Uuid::new_v4())).unwrap();
        let old_thread =
            ThreadKey::parse(format!("test:capacity-old-{}", uuid::Uuid::new_v4())).unwrap();
        let hot_thread =
            ThreadKey::parse(format!("test:capacity-hot-{}", uuid::Uuid::new_v4())).unwrap();
        let trigger_thread =
            ThreadKey::parse(format!("test:capacity-trigger-{}", uuid::Uuid::new_v4())).unwrap();

        store
            .create_or_get_session(
                &stale_thread,
                &HarnessType::Codex,
                None,
                json!({}),
                Default::default(),
            )
            .await
            .expect("create stale session");
        assign_sandbox_identity(&store, &stale_thread, "sbx-stale", "uid-stale").await;
        store
            .create_or_get_session(
                &paused_thread,
                &HarnessType::Codex,
                None,
                json!({}),
                Default::default(),
            )
            .await
            .expect("create paused session");
        assign_sandbox_identity(&store, &paused_thread, "sbx-paused", "uid-paused").await;
        store
            .append_event(
                &paused_thread,
                None,
                "session.sandbox_paused",
                json!({
                    "thread_key": paused_thread.as_str(),
                    "sandbox_id": "sbx-paused",
                    "reason": "capacity_pressure",
                }),
            )
            .await
            .expect("append paused event");
        store
            .create_or_get_session(
                &old_thread,
                &HarnessType::Codex,
                None,
                json!({}),
                Default::default(),
            )
            .await
            .expect("create old session");
        assign_sandbox_identity(&store, &old_thread, "sbx-old", "uid-old").await;
        store
            .create_or_get_session(
                &hot_thread,
                &HarnessType::Codex,
                None,
                json!({}),
                Default::default(),
            )
            .await
            .expect("create hot session");
        assign_sandbox_identity(&store, &hot_thread, "sbx-hot", "uid-hot").await;
        sqlx::query(
            r#"
            update sessions
            set sandbox_last_active_at = case
                    when thread_key = $1 then now() - interval '3 hours'
                    when thread_key = $2 then now() - interval '2 hours'
                    when thread_key = $3 then now() - interval '1 hour'
                end
            where thread_key in ($1, $2, $3)
            "#,
        )
        .bind(stale_thread.as_str())
        .bind(paused_thread.as_str())
        .bind(old_thread.as_str())
        .execute(store.pool())
        .await
        .expect("age capacity candidates");

        let controller = SandboxCapacityController::new(
            store.clone(),
            Arc::new(SandboxManager::new(backend.clone())),
            Arc::new(DashMap::new()),
            SandboxCapacityConfig {
                max_running: 2,
                hot_idle_grace: Duration::from_secs(300),
            },
        );

        controller
            .run_with_capacity(&trigger_thread, "exe-trigger", "cold_create", || async {
                Ok(())
            })
            .await
            .expect("admit under capacity");

        assert_eq!(backend.status_of("sbx-old"), Some(SandboxStatus::Suspended));
        assert_eq!(backend.status_of("sbx-hot"), Some(SandboxStatus::Running));
        assert_eq!(
            store
                .get_session(&stale_thread)
                .await
                .expect("get stale session")
                .sandbox_id,
            None
        );
        assert_eq!(
            store
                .get_session(&paused_thread)
                .await
                .expect("get paused session")
                .sandbox_id
                .as_deref(),
            Some("sbx-paused")
        );
        let old_events = store
            .list_events_after(&old_thread, 0, None, 100)
            .await
            .expect("list old events");
        assert!(old_events.iter().any(|event| {
            event.event_type == "session.sandbox_paused"
                && event.payload.get("reason").and_then(Value::as_str) == Some("capacity_pressure")
        }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn idle_pause_rechecks_for_a_successor_created_while_status_is_blocked() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key = ThreadKey::parse(format!("test:idle-race-{}", Uuid::new_v4())).unwrap();
        let sandbox_id = format!("sbx-idle-race-{}", Uuid::new_v4());
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
        assign_sandbox_identity(&store, &thread_key, &sandbox_id, "uid-idle-race").await;
        let completed = store
            .create_execution(&thread_key, None, json!({}))
            .await
            .unwrap()
            .execution
            .execution_id;
        store
            .complete_execution_if_active(&completed)
            .await
            .unwrap();
        sqlx::query(
            "update session_executions set completed_at = now() - interval '1 hour' where execution_id = $1",
        )
        .bind(&completed)
        .execute(store.pool())
        .await
        .unwrap();

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_status(&sandbox_id, SandboxStatus::Running);
        backend.set_observed_resource_uid(&sandbox_id, "uid-idle-race");
        backend.block_next_status();
        let runtime = runtime_with(&store, backend.clone());
        let context = runtime.context();
        let pause_thread_key = thread_key.clone();
        let pause_sandbox_id = sandbox_id.clone();
        let pause = tokio::spawn(async move {
            record_idle_pause(
                &context,
                &pause_thread_key,
                &completed,
                &pause_sandbox_id,
                Duration::from_secs(60),
            )
            .await
        });

        backend.wait_for_status().await;
        store
            .create_execution(&thread_key, None, json!({}))
            .await
            .unwrap();
        backend.release_status();

        pause.await.unwrap().unwrap();
        assert_eq!(backend.status_of(&sandbox_id), Some(SandboxStatus::Running));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn capacity_pause_serializes_successor_while_exact_pause_is_delayed() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:capacity-race-{}", Uuid::new_v4())).unwrap();
        let sandbox_id = format!("sbx-capacity-race-{}", Uuid::new_v4());
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
        assign_sandbox_identity(&store, &thread_key, &sandbox_id, "uid-capacity-race").await;
        sqlx::query(
            "update sessions set sandbox_last_active_at = now() - interval '1 hour' where thread_key = $1",
        )
        .bind(thread_key.as_str())
        .execute(store.pool())
        .await
        .unwrap();
        let assignment = store
            .lock_sandbox_assignment_for_reconciliation(&thread_key, &sandbox_id)
            .await
            .unwrap()
            .unwrap();
        let epoch = assignment.assignment_epoch().unwrap().to_owned();
        assignment.rollback().await.unwrap();

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_status(&sandbox_id, SandboxStatus::Running);
        backend.set_observed_resource_uid(&sandbox_id, "uid-capacity-race");
        backend.block_next_pause();
        let controller = Arc::new(SandboxCapacityController::new(
            store.clone(),
            Arc::new(SandboxManager::new(backend.clone())),
            Arc::new(DashMap::new()),
            SandboxCapacityConfig {
                max_running: 1,
                hot_idle_grace: Duration::from_secs(60),
            },
        ));
        let candidate = SandboxCapacityCandidate {
            thread_key: thread_key.clone(),
            sandbox_id: sandbox_id.clone(),
            resource_uid: Some("uid-capacity-race".to_owned()),
            assignment_epoch: Some(epoch),
            latest_execution_id: None,
            last_active_at: OffsetDateTime::now_utc() - TimeDuration::hours(1),
        };
        let pause_controller = controller.clone();
        let pause_thread_key = thread_key.clone();
        let pause = tokio::spawn(async move {
            pause_controller
                .pause_capacity_candidate(&candidate, &pause_thread_key, "exe-trigger", "test")
                .await
        });
        backend.wait_for_pause().await;
        let successor_store = store.clone();
        let successor_thread = thread_key.clone();
        let mut successor = tokio::spawn(async move {
            successor_store
                .create_execution(&successor_thread, None, json!({}))
                .await
                .unwrap()
                .execution
                .execution_id
        });
        assert!(
            timeout(Duration::from_millis(50), &mut successor)
                .await
                .is_err()
        );
        backend.release_pause();
        assert!(matches!(
            pause.await.unwrap().unwrap(),
            CapacityCandidateAction::Paused
        ));
        let successor_id = successor.await.unwrap();
        store.complete_execution(&successor_id).await.unwrap();
        assert_eq!(
            backend.status_of(&sandbox_id),
            Some(SandboxStatus::Suspended)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn capacity_pause_skips_when_successor_committed_after_candidate_selection() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:capacity-stale-candidate-{}", Uuid::new_v4())).unwrap();
        let sandbox_id = format!("sbx-capacity-stale-candidate-{}", Uuid::new_v4());
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
        assign_sandbox_identity(
            &store,
            &thread_key,
            &sandbox_id,
            "uid-capacity-stale-candidate",
        )
        .await;
        sqlx::query(
            "update sessions set sandbox_last_active_at = now() - interval '1 hour' where thread_key = $1",
        )
        .bind(thread_key.as_str())
        .execute(store.pool())
        .await
        .unwrap();
        let assignment = store
            .lock_sandbox_assignment_for_reconciliation(&thread_key, &sandbox_id)
            .await
            .unwrap()
            .unwrap();
        let epoch = assignment.assignment_epoch().unwrap().to_owned();
        assignment.rollback().await.unwrap();
        let candidate = SandboxCapacityCandidate {
            thread_key: thread_key.clone(),
            sandbox_id: sandbox_id.clone(),
            resource_uid: Some("uid-capacity-stale-candidate".to_owned()),
            assignment_epoch: Some(epoch),
            latest_execution_id: None,
            last_active_at: OffsetDateTime::now_utc() - TimeDuration::hours(1),
        };

        // The successor commits after the original candidate snapshot but
        // before cleanup holds the assignment row lock.
        let successor = store
            .create_execution(&thread_key, None, json!({}))
            .await
            .unwrap()
            .execution
            .execution_id;
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_status(&sandbox_id, SandboxStatus::Running);
        backend.set_observed_resource_uid(&sandbox_id, "uid-capacity-stale-candidate");
        let controller = SandboxCapacityController::new(
            store.clone(),
            Arc::new(SandboxManager::new(backend.clone())),
            Arc::new(DashMap::new()),
            SandboxCapacityConfig {
                max_running: 1,
                hot_idle_grace: Duration::from_secs(60),
            },
        );

        assert!(matches!(
            controller
                .pause_capacity_candidate(&candidate, &thread_key, "exe-trigger", "test")
                .await
                .unwrap(),
            CapacityCandidateAction::Skipped
        ));
        assert_eq!(backend.status_of(&sandbox_id), Some(SandboxStatus::Running));
        store.complete_execution(&successor).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn workflow_cleanup_stops_and_clears_owned_sandbox() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let workflow_run_id = format!("run-{}", uuid::Uuid::new_v4());
        let sandbox_id = format!("sbx-owned-{}", uuid::Uuid::new_v4());
        let thread_key =
            ThreadKey::parse(format!("test:wf-cleanup-{}", uuid::Uuid::new_v4())).unwrap();
        store
            .create_or_get_session(
                &thread_key,
                &HarnessType::Codex,
                None,
                json!({
                    "source": "absurd_workflow",
                    "workflow_run_id": workflow_run_id,
                    "workflow_owned_thread": true,
                }),
                Default::default(),
            )
            .await
            .expect("create session");
        assign_sandbox_identity(&store, &thread_key, &sandbox_id, "uid-workflow").await;
        store
            .insert_ready_warm_sandbox(&sandbox_id, Some("uid-workflow"), "test-workload")
            .await
            .expect("insert warm sandbox");
        let claimed = store
            .claim_ready_warm_sandbox("test-workload", thread_key.as_str())
            .await
            .expect("claim warm sandbox")
            .expect("warm sandbox exists");
        assert_eq!(claimed.sandbox_id, sandbox_id);
        assert!(
            store
                .list_referenced_sandbox_ids()
                .await
                .expect("list referenced sandboxes")
                .contains(&sandbox_id)
        );

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_resource_uid(&sandbox_id, "uid-workflow");
        let runtime = runtime_with(&store, backend.clone());
        let report = runtime
            .stop_workflow_owned_sandboxes(&workflow_run_id, "test")
            .await
            .expect("cleanup workflow sandboxes");

        assert_eq!(report.stopped, vec![sandbox_id.clone()]);
        assert_eq!(backend.stopped(), vec![sandbox_id.clone()]);
        assert_eq!(
            store.get_session(&thread_key).await.unwrap().sandbox_id,
            None
        );
        assert!(
            !store
                .list_referenced_sandbox_ids()
                .await
                .expect("list referenced sandboxes")
                .contains(&sandbox_id)
        );
        let all = events(&store, &thread_key).await;
        assert!(all.iter().any(|event| {
            event.event_type == "session.workflow_sandbox_stopped"
                && event.payload["workflow_run_id"] == json!(workflow_run_id)
                && event.payload["cleared"] == json!(true)
        }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn workflow_cleanup_does_not_stop_or_clear_a_same_name_replacement() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let workflow_run_id = format!("run-{}", uuid::Uuid::new_v4());
        let sandbox_id = format!("sbx-workflow-aba-{}", uuid::Uuid::new_v4());
        let thread_key = ThreadKey::parse(format!("test:wf-aba-{}", uuid::Uuid::new_v4())).unwrap();
        store
            .create_or_get_session(
                &thread_key,
                &HarnessType::Codex,
                None,
                json!({
                    "source": "absurd_workflow",
                    "workflow_run_id": workflow_run_id,
                    "workflow_owned_thread": true,
                }),
                Default::default(),
            )
            .await
            .expect("create session");
        assign_sandbox_identity(&store, &thread_key, &sandbox_id, "uid-old").await;

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_resource_uid(&sandbox_id, "uid-new");
        let runtime = runtime_with(&store, backend.clone());
        let report = runtime
            .stop_workflow_owned_sandboxes(&workflow_run_id, "test")
            .await
            .expect("stale cleanup reports failure without clearing");

        assert!(report.stopped.is_empty());
        assert_eq!(report.failed.len(), 1);
        assert!(backend.stopped().is_empty());
        assert_eq!(
            store.get_session(&thread_key).await.unwrap().sandbox_id,
            Some(sandbox_id)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn workflow_cleanup_preserves_explicit_unowned_thread_key() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let workflow_run_id = format!("run-{}", uuid::Uuid::new_v4());
        let thread_key =
            ThreadKey::parse(format!("test:wf-explicit-{}", uuid::Uuid::new_v4())).unwrap();
        store
            .create_or_get_session(
                &thread_key,
                &HarnessType::Codex,
                None,
                json!({
                    "source": "absurd_workflow",
                    "workflow_run_id": workflow_run_id,
                }),
                Default::default(),
            )
            .await
            .expect("create session");
        store
            .update_sandbox_id(&thread_key, Some("sbx-explicit"))
            .await
            .expect("set sandbox id");

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let runtime = runtime_with(&store, backend.clone());
        let report = runtime
            .stop_workflow_owned_sandboxes(&workflow_run_id, "test")
            .await
            .expect("cleanup workflow sandboxes");

        assert!(report.stopped.is_empty());
        assert!(backend.stopped().is_empty());
        assert_eq!(
            store.get_session(&thread_key).await.unwrap().sandbox_id,
            Some("sbx-explicit".to_owned())
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn workflow_cleanup_clears_owned_sandbox_when_backend_reports_missing() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let workflow_run_id = format!("run-{}", uuid::Uuid::new_v4());
        let thread_key =
            ThreadKey::parse(format!("test:wf-missing-{}", uuid::Uuid::new_v4())).unwrap();
        store
            .create_or_get_session(
                &thread_key,
                &HarnessType::Codex,
                None,
                json!({
                    "source": "absurd_workflow",
                    "workflow_run_id": workflow_run_id,
                    "workflow_owned_thread": true,
                }),
                Default::default(),
            )
            .await
            .expect("create session");
        assign_sandbox_identity(&store, &thread_key, "sbx-missing", "uid-missing").await;

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.set_observed_resource_uid("sbx-missing", "uid-missing");
        backend.mark_stop_missing("sbx-missing");
        backend.set_observed_status("sbx-missing", SandboxStatus::Gone);
        let runtime = runtime_with(&store, backend);
        let report = runtime
            .stop_workflow_owned_sandboxes(&workflow_run_id, "test")
            .await
            .expect("cleanup workflow sandboxes");

        assert_eq!(report.missing, vec!["sbx-missing".to_owned()]);
        assert_eq!(
            store.get_session(&thread_key).await.unwrap().sandbox_id,
            None
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resume_fence_failure_keeps_assignment_for_retry_without_io() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:resume-failed-{}", uuid::Uuid::new_v4())).unwrap();
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
        assign_sandbox_identity(&store, &thread_key, "sbx-old", "uid-old").await;
        store
            .update_harness_thread_id(&thread_key, Some("harness-thread-1"))
            .await
            .expect("set harness thread id");
        let execution_id = store
            .create_execution(&thread_key, None, json!({}))
            .await
            .expect("create execution")
            .execution
            .execution_id;

        let backend = Arc::new(MockBackend::new(SandboxStatus::Suspended, Vec::new()));
        backend.set_observed_resource_uid("sbx-old", "uid-old");
        backend.fail_resume();
        let runtime = runtime_with(&store, backend.clone());
        assert!(matches!(
            runtime
                .ensure_session_sandbox(EnsureSessionSandboxRequest {
                    thread_key: &thread_key,
                    harness_type: &HarnessType::Codex,
                    persona_id: None,
                    existing_sandbox_id: Some("sbx-old"),
                    existing_sandbox_capabilities: None,
                    iron_control_principal: None,
                    proxy_labels: &BTreeMap::new(),
                    desired_capabilities: &SessionSandboxCapabilities::default_enabled(),
                    execution_metadata: None,
                    execution_id: &execution_id,
                })
                .await,
            Err(SessionRuntimeError::Sandbox(_))
        ));
        let session = store.get_session(&thread_key).await.unwrap();
        assert_eq!(session.sandbox_id, Some("sbx-old".to_owned()));
        assert_eq!(
            session.harness_thread_id,
            Some("harness-thread-1".to_owned())
        );
        assert_eq!(backend.opens(), 0);
        store
            .fail_execution_if_active(&execution_id, "test cleanup")
            .await
            .expect("terminalize test execution");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_pipe_ensure_opens_one_io_per_sandbox() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:pipe-race-{}", uuid::Uuid::new_v4())).unwrap();
        store
            .create_or_get_session(
                &thread_key,
                &HarnessType::Codex,
                None,
                json!({}),
                BTreeMap::new(),
            )
            .await
            .unwrap();
        store
            .update_sandbox_assignment(&thread_key, "sbx-pipe-race", None, &default_capabilities())
            .await
            .unwrap();
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let (first_io, _first_stdout, _first_stdin) = mock_io();
        let (second_io, _second_stdout, _second_stdin) = mock_io();
        backend.push_io(first_io).await;
        backend.push_io(second_io).await;

        let runtime = runtime_with(&store, backend.clone());
        let (first, second) = tokio::join!(
            runtime.ensure_session_pipe(&thread_key, "sbx-pipe-race"),
            runtime.ensure_session_pipe(&thread_key, "sbx-pipe-race"),
        );

        first.expect("first pipe ensure should succeed");
        second.expect("second pipe ensure should reuse the first pipe");
        assert_eq!(backend.opens(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stdout_eof_recovers_terminal_output_from_recorded_logs() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:eof-recorded-{}", uuid::Uuid::new_v4())).unwrap();
        let execution_id =
            orphaned_execution(&store, &thread_key, Some("sbx-recorded"), true).await;

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let (io, stdout, _stdin) = mock_io();
        backend.push_io(io).await;

        let runtime = runtime_with(&store, backend.clone());
        runtime
            .claim_stdout_owner(&execution_id)
            .await
            .expect("claim stdout ownership");
        runtime
            .ensure_session_pipe(&thread_key, "sbx-recorded")
            .await
            .expect("open initial pipe");
        backend.set_recorded_output(completed_output_lines("Recovered from pod logs."));
        drop(stdout);

        wait_for_event(&store, &thread_key, "session.execution_completed").await;
        wait_for_event(&store, &thread_key, "session.stdout_pump_recovered").await;
        let all = events(&store, &thread_key).await;
        assert!(
            all.iter()
                .any(|event| event.event_type == "session.stdout_pump_recovered"),
            "expected recorded-output recovery event"
        );
        assert!(
            !all.iter()
                .any(|event| event.event_type == "session.stdout_pump_reattached"),
            "recorded terminal output should avoid a live reattach"
        );
        assert!(
            !all.iter()
                .any(|event| event.event_type == "session.execution_failed"),
            "stdout eof should not fail an active execution when logs contain a terminal turn"
        );
        let completed = all
            .iter()
            .find(|event| event.event_type == "session.execution_completed")
            .expect("completed event");
        assert_eq!(
            completed.payload["result_text"].as_str(),
            Some("Recovered from pod logs.")
        );
        assert_eq!(backend.opens(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stdout_eof_reattaches_and_delivers_late_terminal_output() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:eof-reattach-{}", uuid::Uuid::new_v4())).unwrap();
        let execution_id =
            orphaned_execution(&store, &thread_key, Some("sbx-reattach"), true).await;

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let (first_io, mut first_stdout, _first_stdin) = mock_io();
        let (second_io, mut second_stdout, _second_stdin) = mock_io();
        backend.push_io(first_io).await;
        backend.push_io(second_io).await;

        let runtime = runtime_with(&store, backend.clone());
        runtime
            .claim_stdout_owner(&execution_id)
            .await
            .expect("claim stdout ownership");
        runtime
            .ensure_session_pipe(&thread_key, "sbx-reattach")
            .await
            .expect("open initial pipe");
        first_stdout
            .write_all(b"{\"type\":\"thread.started\",\"thread_id\":\"mock-thread\"}\n")
            .await
            .unwrap();
        drop(first_stdout);

        wait_for_event(&store, &thread_key, "session.stdout_pump_reattached").await;
        second_stdout
            .write_all(&completed_output_bytes("Completed after reattach."))
            .await
            .unwrap();

        wait_for_event(&store, &thread_key, "session.execution_completed").await;
        let all = events(&store, &thread_key).await;
        assert!(
            !all.iter()
                .any(|event| event.event_type == "session.execution_failed"),
            "reattached stdout should not produce the old false failure"
        );
        let completed = all
            .iter()
            .find(|event| event.event_type == "session.execution_completed")
            .expect("completed event");
        assert_eq!(
            completed.payload["result_text"].as_str(),
            Some("Completed after reattach.")
        );
        assert_eq!(backend.opens(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stdout_resumes_after_ownership_handoff() {
        let Some(store) = test_store().await else {
            return;
        };
        let _serial = TEST_LOCK.lock().await;
        let thread_key =
            ThreadKey::parse(format!("test:stdout-handoff-{}", uuid::Uuid::new_v4())).unwrap();
        let execution_id =
            orphaned_execution(&store, &thread_key, Some("sbx-stdout-handoff"), true).await;
        assert!(
            store
                .claim_stdout_owner(
                    &execution_id,
                    "previous-control-plane",
                    Duration::from_secs(60),
                )
                .await
                .expect("claim previous owner")
        );

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let (io, mut stdout, _stdin) = mock_io();
        backend.push_io(io).await;
        let runtime = runtime_with(&store, backend);
        runtime
            .ensure_session_pipe(&thread_key, "sbx-stdout-handoff")
            .await
            .expect("open stdout pump");

        // A row received during the lease handoff is fenced, but must not
        // permanently disable this pump for the execution.
        stdout
            .write_all(b"{\"type\":\"thread.started\",\"thread_id\":\"handoff-thread\"}\n")
            .await
            .unwrap();
        sleep(Duration::from_millis(100)).await;
        store
            .release_stdout_owner(&execution_id, "previous-control-plane")
            .await
            .expect("release previous owner");
        assert!(
            store
                .claim_stdout_owner(
                    &execution_id,
                    &runtime.stdout_owner_id,
                    Duration::from_secs(60),
                )
                .await
                .expect("claim current owner")
        );

        stdout
            .write_all(&completed_output_bytes(
                "Completed after ownership handoff.",
            ))
            .await
            .unwrap();
        wait_for_event(&store, &thread_key, "session.execution_completed").await;
        let all = events(&store, &thread_key).await;
        let completed = all
            .iter()
            .find(|event| event.event_type == "session.execution_completed")
            .expect("completed event");
        assert_eq!(
            completed.payload["result_text"].as_str(),
            Some("Completed after ownership handoff.")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stdout_eof_fails_when_sandbox_no_longer_accepts_io() {
        let Some(store) = test_store().await else {
            return;
        };
        let _serial = TEST_LOCK.lock().await;
        let thread_key =
            ThreadKey::parse(format!("test:eof-gone-{}", uuid::Uuid::new_v4())).unwrap();
        orphaned_execution(&store, &thread_key, Some("sbx-gone"), true).await;

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let (io, stdout, _stdin) = mock_io();
        backend.push_io(io).await;

        let runtime = runtime_with(&store, backend.clone());
        runtime
            .ensure_session_pipe(&thread_key, "sbx-gone")
            .await
            .expect("open initial pipe");
        backend.set_status(SandboxStatus::Gone);
        drop(stdout);

        wait_for_event(&store, &thread_key, "session.execution_failed").await;
        let all = events(&store, &thread_key).await;
        let failed = all
            .iter()
            .find(|event| event.event_type == "session.execution_failed")
            .expect("failed event");
        let error = failed.payload["error"].as_str().unwrap_or_default();
        assert!(
            error.contains("sandbox stdout closed before terminal output"),
            "unexpected error: {error}"
        );
        assert!(
            error.contains("sandbox no longer accepts io"),
            "expected sandbox status detail: {error}"
        );
        assert!(
            !all.iter()
                .any(|event| event.event_type == "session.stdout_pump_reattached"),
            "gone sandbox should not reattach"
        );
        assert_eq!(backend.opens(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stdout_reattach_preserves_root_state_across_child_terminal() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:eof-root-state-{}", uuid::Uuid::new_v4())).unwrap();
        let execution_id =
            orphaned_execution(&store, &thread_key, Some("sbx-root-state"), true).await;

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let (first_io, mut first_stdout, _first_stdin) = mock_io();
        let (second_io, mut second_stdout, _second_stdin) = mock_io();
        backend.push_io(first_io).await;
        backend.push_io(second_io).await;

        let runtime = runtime_with(&store, backend.clone());
        runtime
            .claim_stdout_owner(&execution_id)
            .await
            .expect("claim stdout ownership");
        runtime
            .ensure_session_pipe(&thread_key, "sbx-root-state")
            .await
            .expect("open initial pipe");
        first_stdout
            .write_all(&output_bytes(&ROOT_START_LINES))
            .await
            .unwrap();
        wait_for_output_line(&store, &thread_key, ROOT_TURN_STARTED_LINE).await;
        backend.set_recorded_output(owned_output_lines(&CHILD_LINES));
        drop(first_stdout);

        wait_for_event(&store, &thread_key, "session.stdout_pump_reattached").await;
        second_stdout
            .write_all(&output_bytes(&CHILD_LINES))
            .await
            .unwrap();
        wait_for_output_line(&store, &thread_key, CHILD_COMPLETED_LINE).await;

        let active = store
            .active_execution_for_thread(&thread_key)
            .await
            .expect("load active execution")
            .expect("child terminal must leave root execution active");
        assert_eq!(active.execution_id, execution_id);
        let session = store.get_session(&thread_key).await.expect("get session");
        assert_eq!(
            session.harness_thread_id.as_deref(),
            Some("root-thread"),
            "child thread start must not replace the root harness thread id"
        );

        second_stdout
            .write_all(&output_bytes(&ROOT_TERMINAL_LINES))
            .await
            .unwrap();

        wait_for_event(&store, &thread_key, "session.execution_completed").await;
        let all = events(&store, &thread_key).await;
        let completed = all
            .iter()
            .find(|event| event.event_type == "session.execution_completed")
            .expect("completed event");
        assert_eq!(
            completed.payload["result_text"].as_str(),
            Some("Root answer.")
        );
        assert_eq!(backend.opens(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn active_eof_reconnect_preserves_root_prefix_and_output_identity_state() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:eof-reconnect-state-{}", uuid::Uuid::new_v4())).unwrap();
        let execution_id =
            orphaned_execution(&store, &thread_key, Some("sbx-eof-reconnect"), true).await;

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let (first_io, mut first_stdout, _first_stdin) = mock_io();
        let (second_io, mut second_stdout, _second_stdin) = mock_io();
        backend.push_io(first_io).await;
        backend.push_io(second_io).await;

        let runtime = runtime_with(&store, backend.clone());
        runtime
            .claim_stdout_owner(&execution_id)
            .await
            .expect("claim stdout ownership");
        let first_pipe = runtime
            .ensure_session_pipe(&thread_key, "sbx-eof-reconnect")
            .await
            .expect("open initial pipe");
        let prefix = r#"{"type":"item.agentMessage.delta","threadId":"root-thread","turnId":"root-turn","itemId":"root-answer","delta":"Hello "}"#;
        first_stdout
            .write_all(&output_bytes(&[
                ROOT_THREAD_STARTED_LINE,
                ROOT_TURN_STARTED_LINE,
                prefix,
            ]))
            .await
            .unwrap();
        wait_for_output_line(&store, &thread_key, prefix).await;

        backend.block_next_recorded_output_read();
        drop(first_stdout);
        backend.wait_for_recorded_output_read().await;
        assert!(
            !first_pipe.stdout_alive.load(Ordering::Acquire),
            "eof must mark the old pipe dead before recorded recovery"
        );

        let session = store.get_session(&thread_key).await.expect("get session");
        let replacement = runtime
            .ensure_session_pipe_with_output_state(
                &thread_key,
                "sbx-eof-reconnect",
                stdout_state_for_execution(&session, &execution_id),
            )
            .await
            .expect("event-stream reconnect should replace the dead pipe");
        assert!(
            !Arc::ptr_eq(&first_pipe.stdin, &replacement.stdin),
            "event-stream reconnect must replace the dead transport"
        );
        assert!(
            Arc::ptr_eq(&first_pipe.output_state, &replacement.output_state),
            "dead-pipe replacement must retain the shared stdout state"
        );
        assert!(
            Arc::ptr_eq(&first_pipe.output_gate, &replacement.output_gate),
            "replacement and retiring pumps must remain behind one output fence"
        );

        second_stdout
            .write_all(&output_bytes(&CHILD_LINES))
            .await
            .unwrap();
        wait_for_output_line(&store, &thread_key, CHILD_COMPLETED_LINE).await;
        let active = store
            .active_execution_for_thread(&thread_key)
            .await
            .expect("load active execution")
            .expect("child terminal must not finish the root execution");
        assert_eq!(active.execution_id, execution_id);

        let suffix = r#"{"type":"item.agentMessage.delta","threadId":"root-thread","turnId":"root-turn","itemId":"root-answer","delta":"world"}"#;
        second_stdout
            .write_all(&output_bytes(&[suffix, ROOT_COMPLETED_LINE]))
            .await
            .unwrap();
        wait_for_execution_event(
            &store,
            &thread_key,
            &execution_id,
            "session.execution_completed",
        )
        .await;
        backend.release_recorded_output_read();

        let completed = store
            .list_events_after(&thread_key, 0, Some(&execution_id), 1000)
            .await
            .expect("list execution events")
            .into_iter()
            .find(|event| event.event_type == "session.execution_completed")
            .expect("completed event");
        assert_eq!(
            completed.payload["result_text"].as_str(),
            Some("Hello world"),
            "the replacement pump must retain the pre-eof answer prefix"
        );
        let session = store.get_session(&thread_key).await.expect("get session");
        assert_eq!(session.harness_thread_id.as_deref(), Some("root-thread"));
        assert_eq!(backend.opens(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reused_pipe_seeds_durable_root_for_second_codex_turn() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:reused-root-state-{}", uuid::Uuid::new_v4())).unwrap();
        let first_execution =
            orphaned_execution(&store, &thread_key, Some("sbx-reused-root"), true).await;

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let (io, mut stdout, _stdin) = mock_io();
        backend.push_io(io).await;
        let runtime = runtime_with(&store, backend.clone());
        runtime
            .claim_stdout_owner(&first_execution)
            .await
            .expect("claim first stdout owner");
        let first_session = store.get_session(&thread_key).await.expect("get session");
        runtime
            .ensure_session_pipe_with_output_state(
                &thread_key,
                "sbx-reused-root",
                stdout_state_for_execution(&first_session, &first_execution),
            )
            .await
            .expect("open first-turn pipe");

        let first_lines = [
            ROOT_THREAD_STARTED_LINE,
            ROOT_TURN_STARTED_LINE,
            ROOT_ANSWER_LINE,
            ROOT_COMPLETED_LINE,
        ];
        stdout.write_all(&output_bytes(&first_lines)).await.unwrap();
        wait_for_execution_event(
            &store,
            &thread_key,
            &first_execution,
            "session.execution_completed",
        )
        .await;

        let created = store
            .create_execution(&thread_key, None, json!({}))
            .await
            .expect("create second execution");
        let second_execution = created.execution.execution_id;
        store
            .mark_execution_running(&second_execution)
            .await
            .expect("mark second execution running");
        runtime
            .claim_stdout_owner(&second_execution)
            .await
            .expect("claim second stdout owner");
        let second_session = store.get_session(&thread_key).await.expect("get session");
        assert_eq!(
            second_session.harness_thread_id.as_deref(),
            Some("root-thread")
        );
        runtime
            .ensure_session_pipe_with_output_state(
                &thread_key,
                "sbx-reused-root",
                stdout_state_for_execution(&second_session, &second_execution),
            )
            .await
            .expect("reuse pipe for second turn");

        stdout.write_all(&output_bytes(&CHILD_LINES)).await.unwrap();
        wait_for_output_line(&store, &thread_key, CHILD_COMPLETED_LINE).await;
        let active = store
            .active_execution_for_thread(&thread_key)
            .await
            .expect("load active execution")
            .expect("child terminal must not finish second root turn");
        assert_eq!(active.execution_id, second_execution);

        let second_answer = r#"{"method":"item/completed","params":{"threadId":"root-thread","turnId":"root-turn-2","item":{"id":"root-answer-2","type":"agentMessage","phase":"final_answer","text":"Second root answer."}}}"#;
        let second_completed = r#"{"method":"turn/completed","params":{"threadId":"root-thread","turn":{"id":"root-turn-2","status":"completed"}}}"#;
        stdout
            .write_all(&output_bytes(&[second_answer, second_completed]))
            .await
            .unwrap();
        wait_for_execution_event(
            &store,
            &thread_key,
            &second_execution,
            "session.execution_completed",
        )
        .await;
        let completed = store
            .list_events_after(&thread_key, 0, Some(&second_execution), 1000)
            .await
            .expect("list second execution events")
            .into_iter()
            .find(|event| event.event_type == "session.execution_completed")
            .expect("second completed event");
        assert_eq!(
            completed.payload["result_text"].as_str(),
            Some("Second root answer.")
        );
        assert_eq!(backend.opens(), 1, "second turn must reuse the pipe");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn eof_liveness_prevents_new_turn_from_reusing_dead_pipe() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:dead-pipe-turn-{}", uuid::Uuid::new_v4())).unwrap();
        let first_execution =
            orphaned_execution(&store, &thread_key, Some("sbx-dead-pipe-turn"), true).await;

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let (first_io, mut first_stdout, _first_stdin) = mock_io();
        let (second_io, mut second_stdout, _second_stdin) = mock_io();
        backend.push_io(first_io).await;
        backend.push_io(second_io).await;
        let runtime = runtime_with(&store, backend.clone());
        runtime
            .claim_stdout_owner(&first_execution)
            .await
            .expect("claim first stdout owner");
        let first_pipe = runtime
            .ensure_session_pipe(&thread_key, "sbx-dead-pipe-turn")
            .await
            .expect("open first pipe");
        first_stdout
            .write_all(&output_bytes(&[
                ROOT_THREAD_STARTED_LINE,
                ROOT_TURN_STARTED_LINE,
                ROOT_ANSWER_LINE,
                ROOT_COMPLETED_LINE,
            ]))
            .await
            .unwrap();
        wait_for_execution_event(
            &store,
            &thread_key,
            &first_execution,
            "session.execution_completed",
        )
        .await;
        drop(first_stdout);

        let deadline = Instant::now() + Duration::from_secs(10);
        while first_pipe.stdout_alive.load(Ordering::Acquire) {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for stdout eof liveness transition"
            );
            sleep(Duration::from_millis(10)).await;
        }

        let created = store
            .create_execution(&thread_key, None, json!({}))
            .await
            .expect("create second execution");
        let second_execution = created.execution.execution_id;
        store
            .mark_execution_running(&second_execution)
            .await
            .expect("mark second execution running");
        runtime
            .claim_stdout_owner(&second_execution)
            .await
            .expect("claim second stdout owner");
        let session = store.get_session(&thread_key).await.expect("get session");
        let second_pipe = runtime
            .ensure_session_pipe_with_output_state(
                &thread_key,
                "sbx-dead-pipe-turn",
                stdout_state_for_execution(&session, &second_execution),
            )
            .await
            .expect("open live pipe for second turn");
        assert!(
            !Arc::ptr_eq(&first_pipe.stdin, &second_pipe.stdin),
            "a dead stdout pipe must never be reused"
        );
        assert_eq!(backend.opens(), 2);

        second_stdout
            .write_all(&completed_output_bytes("Second turn completed."))
            .await
            .unwrap();
        wait_for_execution_event(
            &store,
            &thread_key,
            &second_execution,
            "session.execution_completed",
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn adoption_merges_recovered_state_into_existing_pipe_after_owner_loss() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:adopt-existing-pipe-{}", uuid::Uuid::new_v4())).unwrap();
        let execution_id =
            orphaned_execution(&store, &thread_key, Some("sbx-adopt-existing"), true).await;

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let (io, mut stdout, _stdin) = mock_io();
        backend.push_io(io).await;
        let runtime = runtime_with(&store, backend.clone());
        runtime
            .ensure_session_pipe(&thread_key, "sbx-adopt-existing")
            .await
            .expect("open pipe before adoption");

        // With no stdout owner, the first line is rejected. Adoption must be
        // able to reclaim this same pump rather than leaving it permanently
        // suppressed after that loss.
        stdout
            .write_all(&output_bytes(&[ROOT_THREAD_STARTED_LINE]))
            .await
            .unwrap();
        sleep(Duration::from_millis(200)).await;
        assert!(
            store
                .list_events_after(&thread_key, 0, Some(&execution_id), 1000)
                .await
                .expect("list pre-adoption events")
                .iter()
                .all(|event| event.event_type != SESSION_OUTPUT_LINE_EVENT),
            "unowned pump must not append output"
        );

        store
            .update_harness_thread_id(&thread_key, Some("root-thread"))
            .await
            .expect("persist durable root before restart adoption");
        backend.set_recorded_output(owned_output_lines(&[ROOT_ANSWER_LINE]));
        let orphan = store
            .active_execution_for_thread(&thread_key)
            .await
            .expect("load orphan")
            .expect("orphan remains active");
        assert_eq!(
            runtime
                .adopt_orphaned_execution(&orphan, false, None)
                .await
                .expect("adopt existing pipe"),
            OrphanAdoption::Adopted
        );
        assert_eq!(
            backend.opens(),
            1,
            "adoption must merge into the existing pipe"
        );

        stdout.write_all(&output_bytes(&CHILD_LINES)).await.unwrap();
        wait_for_output_line(&store, &thread_key, CHILD_COMPLETED_LINE).await;
        let active = store
            .active_execution_for_thread(&thread_key)
            .await
            .expect("load active execution")
            .expect("child terminal must leave adopted execution active");
        assert_eq!(active.execution_id, execution_id);

        stdout
            .write_all(&output_bytes(&[ROOT_COMPLETED_LINE]))
            .await
            .unwrap();
        wait_for_execution_event(
            &store,
            &thread_key,
            &execution_id,
            "session.execution_completed",
        )
        .await;
        let completed = store
            .list_events_after(&thread_key, 0, Some(&execution_id), 1000)
            .await
            .expect("list adopted execution events")
            .into_iter()
            .find(|event| event.event_type == "session.execution_completed")
            .expect("adopted completion event");
        assert_eq!(
            completed.payload["result_text"].as_str(),
            Some("Root answer."),
            "recorded final-answer state must survive the existing-pipe merge"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn adoption_fences_existing_pump_until_recovered_root_is_merged() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:adopt-gated-pipe-{}", uuid::Uuid::new_v4())).unwrap();
        let execution_id =
            orphaned_execution(&store, &thread_key, Some("sbx-adopt-gated"), true).await;

        let backend = Arc::new(MockBackend::new(
            SandboxStatus::Running,
            owned_output_lines(&ROOT_START_LINES),
        ));
        let (io, mut stdout, _stdin) = mock_io();
        backend.push_io(io).await;
        let runtime = runtime_with(&store, backend.clone());
        let pipe = runtime
            .ensure_session_pipe(&thread_key, "sbx-adopt-gated")
            .await
            .expect("open pipe before adoption");
        store
            .update_harness_thread_id(&thread_key, Some("root-thread"))
            .await
            .expect("persist durable root");
        backend.block_next_recorded_output_read();
        let orphan = store
            .active_execution_for_thread(&thread_key)
            .await
            .expect("load orphan")
            .expect("orphan remains active");
        let adoption_runtime = runtime.clone();
        let adoption = tokio::spawn(async move {
            adoption_runtime
                .adopt_orphaned_execution(&orphan, false, None)
                .await
        });
        backend.wait_for_recorded_output_read().await;

        let pump_read_wait = pipe.output_gate_read_wait_started.notified();
        stdout.write_all(&output_bytes(&CHILD_LINES)).await.unwrap();
        timeout(Duration::from_secs(2), pump_read_wait)
            .await
            .expect("pump must reach the blocked output-gate read");
        assert!(
            store
                .list_events_after(&thread_key, 0, Some(&execution_id), 1000)
                .await
                .expect("list gated events")
                .iter()
                .all(|event| event.event_type != SESSION_OUTPUT_LINE_EVENT),
            "the pump must stay fenced while adoption reads and seeds recorded state"
        );
        assert_eq!(
            store
                .active_execution_for_thread(&thread_key)
                .await
                .expect("load active execution")
                .expect("child terminal must not race adoption")
                .execution_id,
            execution_id
        );

        backend.release_recorded_output_read();
        assert_eq!(
            adoption
                .await
                .expect("join adoption")
                .expect("adopt existing pump"),
            OrphanAdoption::Adopted
        );
        wait_for_output_line(&store, &thread_key, CHILD_COMPLETED_LINE).await;
        assert!(
            store
                .active_execution_for_thread(&thread_key)
                .await
                .expect("load active execution")
                .is_some(),
            "child terminal must remain non-terminal after recovered root merge"
        );
        stdout
            .write_all(&output_bytes(&ROOT_TERMINAL_LINES))
            .await
            .unwrap();
        wait_for_execution_event(
            &store,
            &thread_key,
            &execution_id,
            "session.execution_completed",
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn adoption_writer_fences_detached_eof_recovery_until_root_merge() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:adopt-gated-eof-{}", uuid::Uuid::new_v4())).unwrap();
        let execution_id =
            orphaned_execution(&store, &thread_key, Some("sbx-adopt-gated-eof"), true).await;

        let backend = Arc::new(MockBackend::new(
            SandboxStatus::Running,
            owned_output_lines(&ROOT_START_LINES),
        ));
        let (first_io, first_stdout, _first_stdin) = mock_io();
        let (second_io, mut second_stdout, _second_stdin) = mock_io();
        backend.push_io(first_io).await;
        backend.push_io(second_io).await;
        let runtime = runtime_with(&store, backend.clone());
        runtime
            .claim_stdout_owner(&execution_id)
            .await
            .expect("claim initial stdout owner");
        let pipe = runtime
            .ensure_session_pipe(&thread_key, "sbx-adopt-gated-eof")
            .await
            .expect("open pipe before adoption");
        expire_stdout_lease(&store, &execution_id).await;

        backend.block_next_recorded_output_read();
        let orphan = store
            .active_execution_for_thread(&thread_key)
            .await
            .expect("load orphan")
            .expect("orphan remains active");
        let adoption_runtime = runtime.clone();
        let adoption = tokio::spawn(async move {
            adoption_runtime
                .adopt_orphaned_execution(&orphan, false, None)
                .await
        });
        backend.wait_for_recorded_output_read().await;
        assert_eq!(backend.recorded_output_reads(), 1);

        backend.set_recorded_output(owned_output_lines(&CHILD_LINES));
        let recovery_wait = pipe.output_gate_read_wait_started.notified();
        drop(first_stdout);
        timeout(Duration::from_secs(2), recovery_wait)
            .await
            .expect("detached recovery must reach the blocked output-gate read");
        assert_eq!(
            backend.recorded_output_reads(),
            2,
            "detached recovery may read logs but must wait before replaying them"
        );
        assert!(
            store
                .active_execution_for_thread(&thread_key)
                .await
                .expect("load fenced execution")
                .is_some(),
            "child-only detached replay must not terminalize behind adoption's writer fence"
        );

        backend.set_recorded_output(owned_output_lines(&ROOT_START_LINES));
        backend.release_recorded_output_read();
        assert_eq!(
            adoption
                .await
                .expect("join adoption")
                .expect("adopt after eof"),
            OrphanAdoption::Adopted
        );

        second_stdout
            .write_all(&output_bytes(&CHILD_LINES))
            .await
            .unwrap();
        wait_for_output_line(&store, &thread_key, CHILD_COMPLETED_LINE).await;
        assert!(
            store
                .active_execution_for_thread(&thread_key)
                .await
                .expect("load active execution")
                .is_some(),
            "child terminal must remain non-terminal after adoption seeds the root"
        );
        second_stdout
            .write_all(&output_bytes(&ROOT_TERMINAL_LINES))
            .await
            .unwrap();
        wait_for_execution_event(
            &store,
            &thread_key,
            &execution_id,
            "session.execution_completed",
        )
        .await;
        assert_eq!(backend.opens(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn adoption_io_deadline_releases_fence_and_owner_without_guessing_root() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };

        let log_thread =
            ThreadKey::parse(format!("test:adopt-log-timeout-{}", uuid::Uuid::new_v4())).unwrap();
        let log_execution =
            orphaned_execution(&store, &log_thread, Some("sbx-adopt-log-timeout"), true).await;
        store
            .update_harness_thread_id(&log_thread, Some("legacy-child"))
            .await
            .expect("persist legacy child-corrupted root");
        let log_backend = Arc::new(MockBackend::new(
            SandboxStatus::Running,
            vec![
                ROOT_THREAD_STARTED_LINE.to_owned(),
                r#"{"method":"thread/started","params":{"thread":{"id":"legacy-child"}}}"#
                    .to_owned(),
            ],
        ));
        let log_runtime = runtime_with(&store, log_backend.clone());
        let log_gate = log_runtime.sandbox_output_gate("sbx-adopt-log-timeout");
        log_backend.block_next_recorded_output_read();
        let orphan = store
            .active_execution_for_thread(&log_thread)
            .await
            .expect("load log-timeout orphan")
            .expect("log-timeout orphan remains active");
        let adoption_runtime = log_runtime.clone();
        let adoption = tokio::spawn(async move {
            adoption_runtime
                .adopt_orphaned_execution(&orphan, false, None)
                .await
        });
        log_backend.wait_for_recorded_output_read().await;
        let gate_read = log_gate.read();
        tokio::pin!(gate_read);
        assert!(
            timeout(Duration::from_millis(50), gate_read.as_mut())
                .await
                .is_err(),
            "recorded-output recovery must hold the writer fence"
        );
        assert_eq!(
            timeout(
                EXECUTION_ADOPTION_IO_TIMEOUT + Duration::from_secs(2),
                adoption,
            )
            .await
            .expect("log-timeout adoption must stay bounded")
            .expect("join log-timeout adoption")
            .expect("log-timeout adoption result"),
            OrphanAdoption::Deferred
        );
        let gate_read = timeout(Duration::from_millis(250), gate_read)
            .await
            .expect("log timeout must release the writer fence");
        drop(gate_read);
        log_backend.release_recorded_output_read();
        assert_eq!(
            store
                .get_session(&log_thread)
                .await
                .expect("get log-timeout session")
                .harness_thread_id
                .as_deref(),
            Some("legacy-child"),
            "timed-out recorded output must not repair a root from unseen evidence"
        );
        let log_successor = runtime_with(&store, log_backend);
        assert!(
            log_successor
                .claim_expired_stdout_owner(&log_execution)
                .await
                .expect("claim released log-timeout owner"),
            "a successor must be able to claim immediately after log timeout"
        );
        store
            .fail_execution_if_active(&log_execution, "test cleanup")
            .await
            .expect("terminalize log-timeout execution");

        let attach_thread = ThreadKey::parse(format!(
            "test:adopt-attach-timeout-{}",
            uuid::Uuid::new_v4()
        ))
        .unwrap();
        let attach_execution = orphaned_execution(
            &store,
            &attach_thread,
            Some("sbx-adopt-attach-timeout"),
            true,
        )
        .await;
        let attach_backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let (io, _stdout, _stdin) = mock_io();
        attach_backend.push_io(io).await;
        attach_backend.block_next_open_io();
        let attach_runtime = runtime_with(&store, attach_backend.clone());
        let attach_gate = attach_runtime.sandbox_output_gate("sbx-adopt-attach-timeout");
        let orphan = store
            .active_execution_for_thread(&attach_thread)
            .await
            .expect("load attach-timeout orphan")
            .expect("attach-timeout orphan remains active");
        let adoption_runtime = attach_runtime.clone();
        let adoption = tokio::spawn(async move {
            adoption_runtime
                .adopt_orphaned_execution(&orphan, false, None)
                .await
        });
        attach_backend.wait_for_open_io().await;
        let gate_read = attach_gate.read();
        tokio::pin!(gate_read);
        assert!(
            timeout(Duration::from_millis(50), gate_read.as_mut())
                .await
                .is_err(),
            "sandbox attach must remain inside the writer fence"
        );
        assert_eq!(
            timeout(
                EXECUTION_ADOPTION_IO_TIMEOUT + Duration::from_secs(2),
                adoption,
            )
            .await
            .expect("attach-timeout adoption must stay bounded")
            .expect("join attach-timeout adoption")
            .expect("attach-timeout adoption result"),
            OrphanAdoption::Deferred
        );
        let gate_read = timeout(Duration::from_millis(250), gate_read)
            .await
            .expect("attach timeout must release the writer fence");
        drop(gate_read);
        attach_backend.release_open_io();
        assert!(
            attach_runtime
                .sandbox_pipes
                .get("sbx-adopt-attach-timeout")
                .is_none(),
            "a cancelled attach must not publish a partial pipe"
        );
        assert!(
            attach_runtime.sandbox_pipe_open_locks.is_empty(),
            "a cancelled attach must reap its per-sandbox open lock"
        );
        let attach_successor = runtime_with(&store, attach_backend);
        assert!(
            attach_successor
                .claim_expired_stdout_owner(&attach_execution)
                .await
                .expect("claim released attach-timeout owner"),
            "a successor must be able to claim immediately after attach timeout"
        );
        store
            .fail_execution_if_active(&attach_execution, "test cleanup")
            .await
            .expect("terminalize attach-timeout execution");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn adoption_release_timeout_stops_renewer_before_lease_expires() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key = ThreadKey::parse(format!(
            "test:adopt-release-timeout-{}",
            uuid::Uuid::new_v4()
        ))
        .unwrap();
        let execution_id =
            orphaned_execution(&store, &thread_key, Some("sbx-adopt-release-timeout"), true).await;
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        backend.block_next_recorded_output_read();
        let runtime = runtime_with(&store, backend.clone());
        let orphan = store
            .active_execution_for_thread(&thread_key)
            .await
            .expect("load release-timeout orphan")
            .expect("release-timeout orphan remains active");
        let adoption_runtime = runtime.clone();
        let adoption = tokio::spawn(async move {
            adoption_runtime
                .adopt_orphaned_execution(&orphan, false, None)
                .await
        });
        backend.wait_for_recorded_output_read().await;
        assert!(
            runtime.stdout_owner_renewals.contains_key(&execution_id),
            "adoption must renew ownership while backend recovery is in flight"
        );

        sqlx::query(
            "update session_executions \
             set stdout_owner_lease_expires_at = clock_timestamp() + interval '300 milliseconds' \
             where execution_id = $1",
        )
        .bind(&execution_id)
        .execute(store.pool())
        .await
        .expect("shorten adoption lease");
        let mut row_lock = store.pool().begin().await.expect("begin owner row lock");
        sqlx::query(
            "select execution_id from session_executions where execution_id = $1 for update",
        )
        .bind(&execution_id)
        .fetch_one(&mut *row_lock)
        .await
        .expect("lock owner row");

        timeout(
            EXECUTION_ADOPTION_IO_TIMEOUT + Duration::from_secs(1),
            runtime.stdout_owner_release_started.notified(),
        )
        .await
        .expect("adoption must reach bounded owner release");
        assert!(
            !runtime.stdout_owner_renewals.contains_key(&execution_id),
            "the renewer must stop before a possibly blocked release starts"
        );
        assert_eq!(
            timeout(Duration::from_secs(1), adoption)
                .await
                .expect("blocked release must stay bounded")
                .expect("join release-timeout adoption")
                .expect("release-timeout adoption result"),
            OrphanAdoption::Deferred
        );
        row_lock.rollback().await.expect("release owner row lock");
        backend.release_recorded_output_read();

        assert!(
            store
                .claim_expired_stdout_owner(
                    &execution_id,
                    "peer-control-plane",
                    Duration::from_secs(5),
                )
                .await
                .expect("peer claims naturally expired lease"),
            "a failed owner release must become claimable by lease expiry"
        );
        store
            .fail_execution_if_active(&execution_id, "test cleanup")
            .await
            .expect("terminalize release-timeout execution");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn adoption_root_persistence_error_releases_owner_without_attaching() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key = ThreadKey::parse(format!(
            "test:adopt-root-store-error-{}",
            uuid::Uuid::new_v4()
        ))
        .unwrap();
        let execution_id =
            orphaned_execution(&store, &thread_key, Some("sbx-adopt-root-error"), true).await;
        let backend = Arc::new(MockBackend::new(
            SandboxStatus::Running,
            owned_output_lines(&ROOT_START_LINES),
        ));
        let runtime = runtime_with(&store, backend.clone());
        runtime
            .fail_adoption_root_persistence
            .store(true, Ordering::SeqCst);
        let orphan = store
            .active_execution_for_thread(&thread_key)
            .await
            .expect("load root-persistence orphan")
            .expect("root-persistence orphan remains active");

        let error = runtime
            .adopt_orphaned_execution(&orphan, false, None)
            .await
            .expect_err("root persistence failure must stop adoption");
        assert!(
            matches!(
                error,
                SessionRuntimeError::Store(SessionStoreError::InvalidPersistedValue(_))
            ),
            "unexpected adoption error: {error}"
        );
        assert_eq!(
            backend.opens(),
            0,
            "adoption must not attach with a process-local-only recovered root"
        );
        assert_eq!(
            store
                .get_session(&thread_key)
                .await
                .expect("get root-persistence session")
                .harness_thread_id,
            None
        );

        let successor = runtime_with(&store, backend);
        assert!(
            successor
                .claim_expired_stdout_owner(&execution_id)
                .await
                .expect("successor claims released root-persistence execution"),
            "root persistence failure must release ownership for retry"
        );
        store
            .fail_execution_if_active(&execution_id, "test cleanup")
            .await
            .expect("terminalize root-persistence execution");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lease_losing_pump_does_not_persist_harness_thread_id() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:lost-owner-root-{}", uuid::Uuid::new_v4())).unwrap();
        let execution_id =
            orphaned_execution(&store, &thread_key, Some("sbx-lost-owner-root"), true).await;
        store
            .claim_stdout_owner(&execution_id, "peer-control-plane", Duration::from_secs(60))
            .await
            .expect("claim peer stdout owner");

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let (io, mut stdout, _stdin) = mock_io();
        backend.push_io(io).await;
        let runtime = runtime_with(&store, backend);
        runtime
            .ensure_session_pipe(&thread_key, "sbx-lost-owner-root")
            .await
            .expect("open losing pump");
        stdout
            .write_all(&output_bytes(&[ROOT_THREAD_STARTED_LINE]))
            .await
            .unwrap();
        sleep(Duration::from_millis(200)).await;

        let session = store.get_session(&thread_key).await.expect("get session");
        assert_eq!(
            session.harness_thread_id, None,
            "a pump that cannot append as stdout owner must not persist its inferred root"
        );
        assert!(
            store
                .list_events_after(&thread_key, 0, Some(&execution_id), 1000)
                .await
                .expect("list execution events")
                .iter()
                .all(|event| event.event_type != SESSION_OUTPUT_LINE_EVENT),
            "peer-owned output must not be appended"
        );
        store
            .fail_execution_if_active(&execution_id, "test cleanup")
            .await
            .expect("terminalize peer-owned execution");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn expired_owner_pump_failure_does_not_claim_terminalization() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key = ThreadKey::parse(format!(
            "test:expired-pump-failure-{}",
            uuid::Uuid::new_v4()
        ))
        .unwrap();
        let execution_id =
            orphaned_execution(&store, &thread_key, Some("sbx-expired-pump"), true).await;
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let runtime = runtime_with(&store, backend.clone());
        assert!(
            store
                .claim_stdout_owner(
                    &execution_id,
                    &runtime.stdout_owner_id,
                    Duration::from_secs(60),
                )
                .await
                .expect("claim pump owner")
        );
        expire_stdout_lease(&store, &execution_id).await;

        record_stdout_pump_failure(
            &runtime.context(),
            &thread_key,
            "sbx-expired-pump",
            "sandbox stdout line exceeded codec maximum length".to_owned(),
        )
        .await
        .expect("record honest pump failure");

        let all = store
            .list_events_after(&thread_key, 0, Some(&execution_id), 1000)
            .await
            .expect("list pump-failure events");
        let pump_failure = all
            .iter()
            .find(|event| event.event_type == "session.stdout_pump_failed")
            .expect("pump-failure diagnostic");
        assert_eq!(
            pump_failure.payload["terminalized_execution"].as_bool(),
            Some(false)
        );
        assert!(
            all.iter()
                .all(|event| event.event_type != "session.execution_failed"),
            "an expired owner must not emit a terminal failure event"
        );
        assert!(
            store
                .active_execution_for_thread(&thread_key)
                .await
                .expect("load active execution")
                .is_some(),
            "the diagnostic must not claim a rejected terminal transition"
        );

        let successor = runtime_with(&store, backend);
        assert!(
            successor
                .claim_expired_stdout_owner(&execution_id)
                .await
                .expect("successor claims expired execution")
        );
        store
            .fail_execution_if_active(&execution_id, "test cleanup")
            .await
            .expect("terminalize expired-pump execution");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stdout_eof_fails_when_sandbox_no_longer_accepts_io() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:eof-gone-{}", uuid::Uuid::new_v4())).unwrap();
        let execution_id = orphaned_execution(&store, &thread_key, Some("sbx-gone"), true).await;

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let (io, stdout, _stdin) = mock_io();
        backend.push_io(io).await;

        let runtime = runtime_with(&store, backend.clone());
        runtime
            .claim_stdout_owner(&execution_id)
            .await
            .expect("claim stdout ownership");
        runtime
            .ensure_session_pipe(&thread_key, "sbx-gone")
            .await
            .expect("open initial pipe");
        backend.set_status(SandboxStatus::Gone);
        drop(stdout);

        wait_for_event(&store, &thread_key, "session.execution_failed").await;
        let all = events(&store, &thread_key).await;
        let failed = all
            .iter()
            .find(|event| event.event_type == "session.execution_failed")
            .expect("failed event");
        let error = failed.payload["error"].as_str().unwrap_or_default();
        assert!(
            error.contains("sandbox stdout closed before terminal output"),
            "unexpected error: {error}"
        );
        assert!(
            error.contains("sandbox no longer accepts io"),
            "expected sandbox status detail: {error}"
        );
        assert!(
            !all.iter()
                .any(|event| event.event_type == "session.stdout_pump_reattached"),
            "gone sandbox should not reattach"
        );
        assert_eq!(backend.opens(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn adopts_finished_turn_from_recorded_sandbox_output() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:adopt-logs-{}", uuid::Uuid::new_v4())).unwrap();
        orphaned_execution(&store, &thread_key, Some("sbx-mock"), true).await;

        let backend = Arc::new(MockBackend::new(
            SandboxStatus::Running,
            vec![
                json!({"type": "item.completed", "item": {"id": "msg-1", "type": "agentMessage", "text": "Done: pushed commit abc123.", "phase": "final_answer"}}).to_string(),
                json!({"type": "turn.completed", "turn": {"id": "turn-1", "status": "completed"}}).to_string(),
            ],
        ));
        let runtime = runtime_with(&store, backend.clone());
        runtime.adopt_orphaned_executions().await;

        wait_for_event(&store, &thread_key, "session.execution_completed").await;
        let all = events(&store, &thread_key).await;
        assert!(
            all.iter()
                .any(|event| event.event_type == "session.execution_adopted"),
            "expected an adoption event"
        );
        let completed = all
            .iter()
            .find(|event| event.event_type == "session.execution_completed")
            .expect("completed event");
        assert_eq!(
            completed.payload["result_text"].as_str(),
            Some("Done: pushed commit abc123.")
        );
        // The terminal came from recorded output; no live attach was needed.
        assert_eq!(backend.opens(), 0);
        let session = store.get_session(&thread_key).await.unwrap();
        assert_ne!(session.status.as_ref(), "failed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn adopts_live_when_recorded_output_has_no_terminal() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:adopt-live-{}", uuid::Uuid::new_v4())).unwrap();
        orphaned_execution(&store, &thread_key, Some("sbx-mock"), true).await;

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let (io, mut stdout, _stdin) = mock_io();
        backend.push_io(io).await;

        let runtime = runtime_with(&store, backend.clone());
        runtime.adopt_orphaned_executions().await;
        assert_eq!(backend.opens(), 1);

        stdout
            .write_all(
                b"{\"type\":\"turn.completed\",\"turn\":{\"id\":\"turn-1\",\"status\":\"completed\"}}\n",
            )
            .await
            .unwrap();
        wait_for_event(&store, &thread_key, "session.execution_completed").await;
        let all = events(&store, &thread_key).await;
        assert!(
            all.iter().any(|event| {
                event.event_type == "session.execution_adopted"
                    && event.payload["mode"] == json!("live_attach")
            }),
            "expected a live adoption event"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn adoption_replays_root_state_before_live_child_terminal() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:adopt-root-state-{}", uuid::Uuid::new_v4())).unwrap();
        let execution_id =
            orphaned_execution(&store, &thread_key, Some("sbx-adopt-root"), true).await;

        let backend = Arc::new(MockBackend::new(
            SandboxStatus::Running,
            owned_output_lines(&ROOT_START_LINES),
        ));
        let (io, mut stdout, _stdin) = mock_io();
        backend.push_io(io).await;

        let runtime = runtime_with(&store, backend.clone());
        runtime.adopt_orphaned_executions().await;
        assert_eq!(backend.opens(), 1);

        stdout.write_all(&output_bytes(&CHILD_LINES)).await.unwrap();
        wait_for_output_line(&store, &thread_key, CHILD_COMPLETED_LINE).await;

        let active = store
            .active_execution_for_thread(&thread_key)
            .await
            .expect("load active execution")
            .expect("child terminal must leave adopted root execution active");
        assert_eq!(active.execution_id, execution_id);
        let session = store.get_session(&thread_key).await.expect("get session");
        assert_eq!(
            session.harness_thread_id.as_deref(),
            Some("root-thread"),
            "recorded root identity must survive live child output"
        );

        stdout
            .write_all(&output_bytes(&ROOT_TERMINAL_LINES))
            .await
            .unwrap();

        wait_for_event(&store, &thread_key, "session.execution_completed").await;
        let all = events(&store, &thread_key).await;
        let completed = all
            .iter()
            .find(|event| event.event_type == "session.execution_completed")
            .expect("completed event");
        assert_eq!(
            completed.payload["result_text"].as_str(),
            Some("Root answer.")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn adoption_repairs_legacy_child_corrupted_durable_root() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:repair-legacy-root-{}", uuid::Uuid::new_v4())).unwrap();
        let execution_id =
            orphaned_execution(&store, &thread_key, Some("sbx-repair-root"), true).await;
        store
            .update_harness_thread_id(&thread_key, Some("child-thread"))
            .await
            .expect("persist legacy child-corrupted root");

        let recorded = ROOT_START_LINES
            .into_iter()
            .chain(CHILD_LINES)
            .chain(ROOT_TERMINAL_LINES)
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, recorded));
        let runtime = runtime_with(&store, backend.clone());
        let orphan = store
            .active_execution_for_thread(&thread_key)
            .await
            .expect("load orphan")
            .expect("orphan remains active");
        assert_eq!(
            runtime
                .adopt_orphaned_execution(&orphan, false, None)
                .await
                .expect("adopt recorded execution"),
            OrphanAdoption::Adopted
        );
        wait_for_execution_event(
            &store,
            &thread_key,
            &execution_id,
            "session.execution_completed",
        )
        .await;

        let session = store.get_session(&thread_key).await.expect("get session");
        assert_eq!(
            session.harness_thread_id.as_deref(),
            Some("root-thread"),
            "recorded root-before-child evidence must repair the legacy durable child id"
        );
        let completed = store
            .list_events_after(&thread_key, 0, Some(&execution_id), 1000)
            .await
            .expect("list execution events")
            .into_iter()
            .find(|event| event.event_type == "session.execution_completed")
            .expect("completed event");
        assert_eq!(
            completed.payload["result_text"].as_str(),
            Some("Root answer.")
        );
        assert_eq!(backend.opens(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fails_orphans_whose_sandbox_is_gone() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:adopt-gone-{}", uuid::Uuid::new_v4())).unwrap();
        orphaned_execution(&store, &thread_key, Some("sbx-mock"), true).await;

        let backend = Arc::new(MockBackend::new(SandboxStatus::Gone, Vec::new()));
        let runtime = runtime_with(&store, backend.clone());
        runtime.adopt_orphaned_executions().await;

        wait_for_event(&store, &thread_key, "session.execution_failed").await;
        let all = events(&store, &thread_key).await;
        let failed = all
            .iter()
            .find(|event| event.event_type == "session.execution_failed")
            .expect("failed event");
        let error = failed.payload["error"].as_str().unwrap_or_default();
        assert!(
            error.contains("execution orphaned by control plane restart"),
            "unexpected error: {error}"
        );
        assert!(
            error.contains("sandbox no longer accepts io"),
            "expected status detail: {error}"
        );
        assert_eq!(backend.opens(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fails_queued_orphans_that_never_received_input() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:adopt-queued-{}", uuid::Uuid::new_v4())).unwrap();
        orphaned_execution(&store, &thread_key, Some("sbx-mock"), false).await;

        // The one-shot scan has no later tick to revisit skipped rows, so it
        // fails queued orphans immediately regardless of age.
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let runtime = runtime_with(&store, backend.clone());
        runtime.adopt_orphaned_executions().await;

        wait_for_event(&store, &thread_key, "session.execution_failed").await;
        let all = events(&store, &thread_key).await;
        let failed = all
            .iter()
            .find(|event| event.event_type == "session.execution_failed")
            .expect("failed event");
        let error = failed.payload["error"].as_str().unwrap_or_default();
        assert!(
            error.contains("orphaned before input was sent"),
            "unexpected error: {error}"
        );
        assert_eq!(backend.opens(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn periodic_scan_skips_young_pre_sandbox_executions_until_grace_passes() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:adopt-young-{}", uuid::Uuid::new_v4())).unwrap();
        let execution_id = orphaned_execution(&store, &thread_key, Some("sbx-mock"), false).await;

        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let runtime = runtime_with(&store, backend.clone());
        let mut state = OrphanAdoptionState::default();
        runtime
            .run_orphan_adoption_scan(&mut state, Some(PRE_SANDBOX_ORPHAN_GRACE))
            .await;

        // A queued row younger than the grace window may belong to a live
        // execute_session mid-transition; a periodic scan must leave it
        // alone and revisit it later.
        let all = events(&store, &thread_key).await;
        assert!(
            all.iter()
                .all(|event| event.event_type != "session.execution_failed"),
            "young queued execution must not be failed"
        );
        let active = store
            .list_active_executions()
            .await
            .expect("list active executions");
        assert!(
            active
                .iter()
                .any(|execution| execution.execution_id == execution_id),
            "young queued execution must stay active"
        );

        // Once the row ages past the grace window, a later tick fails it.
        backdate_execution(&store, &execution_id, 300.0).await;
        runtime
            .run_orphan_adoption_scan(&mut state, Some(PRE_SANDBOX_ORPHAN_GRACE))
            .await;
        wait_for_event(&store, &thread_key, "session.execution_failed").await;

        // A newly running execution can still be waiting for its warm
        // sandbox assignment. It gets the same periodic grace, but is failed
        // if it remains unassigned after the grace window.
        let running_thread =
            ThreadKey::parse(format!("test:adopt-young-running-{}", uuid::Uuid::new_v4())).unwrap();
        let running_execution = orphaned_execution(&store, &running_thread, None, true).await;
        runtime
            .run_orphan_adoption_scan(&mut state, Some(PRE_SANDBOX_ORPHAN_GRACE))
            .await;
        assert!(
            events(&store, &running_thread)
                .await
                .iter()
                .all(|event| event.event_type != "session.execution_failed"),
            "young running execution must survive sandbox assignment"
        );

        backdate_execution(&store, &running_execution, 300.0).await;
        runtime
            .run_orphan_adoption_scan(&mut state, Some(PRE_SANDBOX_ORPHAN_GRACE))
            .await;
        wait_for_event(&store, &running_thread, "session.execution_failed").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn adopts_deferred_execution_after_lease_expires() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:adopt-deferred-{}", uuid::Uuid::new_v4())).unwrap();
        let execution_id = orphaned_execution(&store, &thread_key, Some("sbx-mock"), true).await;
        store
            .claim_stdout_owner(
                &execution_id,
                "other-control-plane",
                Duration::from_secs(60),
            )
            .await
            .expect("claim lease for other owner");

        let backend = Arc::new(MockBackend::new(
            SandboxStatus::Running,
            vec![
                json!({"type": "item.completed", "item": {"id": "msg-1", "type": "agentMessage", "text": "Done: recovered after handoff.", "phase": "final_answer"}}).to_string(),
                json!({"type": "turn.completed", "turn": {"id": "turn-1", "status": "completed"}}).to_string(),
            ],
        ));
        let runtime = runtime_with(&store, backend.clone());

        // While another control plane holds the stdout-owner lease the scan
        // must defer instead of stealing the execution.
        runtime.adopt_orphaned_executions().await;
        wait_for_event(&store, &thread_key, "session.execution_adoption_deferred").await;
        let all = events(&store, &thread_key).await;
        assert!(
            all.iter()
                .all(|event| event.event_type != "session.execution_completed"),
            "deferred execution must not be terminalized"
        );

        // Once the lease expires (owner died without releasing), a later
        // scan adopts the execution and recovers the recorded terminal. The
        // expiry is forced in the database rather than slept through so slow
        // test databases cannot turn the first scan into the adopting one.
        expire_stdout_lease(&store, &execution_id).await;
        runtime.adopt_orphaned_executions().await;
        wait_for_event(&store, &thread_key, "session.execution_adopted").await;
        wait_for_event(&store, &thread_key, "session.execution_completed").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn periodic_scan_ignores_live_self_owner_then_adopts_it_after_expiry() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:adopt-own-{}", uuid::Uuid::new_v4())).unwrap();
        let execution_id = orphaned_execution(&store, &thread_key, Some("sbx-mock"), true).await;
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let runtime = runtime_with(&store, backend.clone());
        assert!(
            store
                .claim_stdout_owner(
                    &execution_id,
                    &runtime.stdout_owner_id,
                    Duration::from_secs(60)
                )
                .await
                .expect("claim as this control plane")
        );

        // A healthy execution owned by the scanning process must be skipped
        // silently: no deferral event, no sandbox status probe.
        let mut state = OrphanAdoptionState::default();
        runtime
            .run_orphan_adoption_scan(&mut state, Some(PRE_SANDBOX_ORPHAN_GRACE))
            .await;
        runtime
            .run_orphan_adoption_scan(&mut state, Some(PRE_SANDBOX_ORPHAN_GRACE))
            .await;

        let all = events(&store, &thread_key).await;
        assert!(
            all.iter().all(|event| {
                event.event_type != "session.execution_adoption_deferred"
                    && event.event_type != "session.execution_adopted"
                    && event.event_type != "session.execution_failed"
            }),
            "self-owned execution must not be touched by the scan"
        );

        backend.set_recorded_output(completed_output_lines(
            "Recovered after this process lost its lease.",
        ));
        expire_stdout_lease(&store, &execution_id).await;
        runtime
            .run_orphan_adoption_scan(&mut state, Some(PRE_SANDBOX_ORPHAN_GRACE))
            .await;
        wait_for_execution_event(
            &store,
            &thread_key,
            &execution_id,
            "session.execution_completed",
        )
        .await;
        let completed = store
            .list_events_after(&thread_key, 0, Some(&execution_id), 1000)
            .await
            .expect("list self-owner adoption events")
            .into_iter()
            .find(|event| event.event_type == "session.execution_completed")
            .expect("self-owner expiry completion");
        assert_eq!(
            completed.payload["result_text"].as_str(),
            Some("Recovered after this process lost its lease."),
            "the same process must reclaim its expired lease through adoption"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn same_runtime_reclaim_stops_the_old_renewer_generation() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:renewer-generation-{}", uuid::Uuid::new_v4())).unwrap();
        let execution_id =
            orphaned_execution(&store, &thread_key, Some("sbx-renewer-generation"), true).await;
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let runtime = runtime_with(&store, backend);
        runtime
            .claim_stdout_owner(&execution_id)
            .await
            .expect("claim initial generation");
        let first = runtime
            .stdout_owner_renewals
            .get(&execution_id)
            .map(|entry| Arc::clone(entry.value()))
            .expect("initial renewer");

        expire_stdout_lease(&store, &execution_id).await;
        assert!(
            runtime
                .claim_expired_stdout_owner(&execution_id)
                .await
                .expect("reclaim expired same-owner lease")
        );
        let second = runtime
            .stdout_owner_renewals
            .get(&execution_id)
            .map(|entry| Arc::clone(entry.value()))
            .expect("replacement renewer");
        assert_ne!(first.generation, second.generation);
        assert!(
            *first.stopped.borrow(),
            "the old generation must finish before the database reclaim"
        );
        tokio::task::yield_now().await;
        assert_eq!(
            runtime
                .stdout_owner_renewals
                .get(&execution_id)
                .map(|entry| entry.generation),
            Some(second.generation),
            "old-generation cleanup must not remove the replacement"
        );

        assert!(stop_stdout_owner_renewer(&runtime.stdout_owner_renewals, &execution_id).await);
        store
            .fail_execution_if_active(&execution_id, "test cleanup")
            .await
            .expect("terminalize renewer-generation execution");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abandon_fails_closed_while_a_renewal_is_in_flight() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:renewer-in-flight-{}", uuid::Uuid::new_v4())).unwrap();
        let execution_id =
            orphaned_execution(&store, &thread_key, Some("sbx-renewer-in-flight"), true).await;
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let runtime = runtime_with(&store, backend);
        runtime
            .claim_stdout_owner(&execution_id)
            .await
            .expect("claim stdout owner");
        let renewal = runtime
            .stdout_owner_renewals
            .get(&execution_id)
            .map(|entry| Arc::clone(entry.value()))
            .expect("active renewer");

        let mut row_lock = store.pool().begin().await.expect("begin owner row lock");
        sqlx::query(
            "select execution_id from session_executions where execution_id = $1 for update",
        )
        .bind(&execution_id)
        .fetch_one(&mut *row_lock)
        .await
        .expect("lock owner row");
        renewal.renew_now.notify_one();
        timeout(Duration::from_secs(1), renewal.renew_db_started.notified())
            .await
            .expect("renewer must reach its database update");

        let release_started = runtime.stdout_owner_release_started.notified();
        tokio::pin!(release_started);
        assert!(
            !runtime.abandon_stdout_owner(&execution_id).await,
            "a blocked renewal must make abandonment fail closed"
        );
        assert!(
            runtime.stdout_owner_renewals.contains_key(&execution_id),
            "the in-flight generation must stay registered until its update resolves"
        );
        assert!(
            timeout(Duration::from_millis(50), release_started.as_mut())
                .await
                .is_err(),
            "lease release must not start until the renewer is known to be stopped"
        );

        row_lock.rollback().await.expect("release owner row lock");
        timeout(Duration::from_secs(1), renewal.wait_stopped())
            .await
            .expect("cancelled renewer must stop after its database update");
        assert!(
            runtime.abandon_stdout_owner(&execution_id).await,
            "abandonment may release once the old generation is stopped"
        );
        assert!(
            store
                .claim_stdout_owner(&execution_id, "peer-control-plane", Duration::from_secs(5),)
                .await
                .expect("peer claims released owner")
        );
        store
            .fail_execution_if_active(&execution_id, "test cleanup")
            .await
            .expect("terminalize in-flight-renewer execution");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spawned_adoption_loop_recovers_orphans() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:adopt-loop-{}", uuid::Uuid::new_v4())).unwrap();
        orphaned_execution(&store, &thread_key, Some("sbx-mock"), true).await;

        let backend = Arc::new(MockBackend::new(
            SandboxStatus::Running,
            vec![
                json!({"type": "item.completed", "item": {"id": "msg-1", "type": "agentMessage", "text": "Done: recovered by the loop.", "phase": "final_answer"}}).to_string(),
                json!({"type": "turn.completed", "turn": {"id": "turn-1", "status": "completed"}}).to_string(),
            ],
        ));
        let runtime = runtime_with(&store, backend.clone());
        runtime.spawn_orphan_adoption(Duration::from_millis(50));

        wait_for_event(&store, &thread_key, "session.execution_adopted").await;
        wait_for_event(&store, &thread_key, "session.execution_completed").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn periodic_scans_record_deferral_once() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:adopt-dedup-{}", uuid::Uuid::new_v4())).unwrap();
        let execution_id = orphaned_execution(&store, &thread_key, Some("sbx-mock"), true).await;
        store
            .claim_stdout_owner(
                &execution_id,
                "other-control-plane",
                Duration::from_secs(60),
            )
            .await
            .expect("claim lease for other owner");

        let backend = Arc::new(MockBackend::new(
            SandboxStatus::Running,
            vec![
                json!({"type": "item.completed", "item": {"id": "msg-1", "type": "agentMessage", "text": "Done: recovered after release.", "phase": "final_answer"}}).to_string(),
                json!({"type": "turn.completed", "turn": {"id": "turn-1", "status": "completed"}}).to_string(),
            ],
        ));
        let runtime = runtime_with(&store, backend.clone());

        // Repeated periodic scans over the same held lease must record the
        // deferral event only once.
        let mut state = OrphanAdoptionState::default();
        runtime
            .run_orphan_adoption_scan(&mut state, Some(PRE_SANDBOX_ORPHAN_GRACE))
            .await;
        runtime
            .run_orphan_adoption_scan(&mut state, Some(PRE_SANDBOX_ORPHAN_GRACE))
            .await;
        let all = events(&store, &thread_key).await;
        let deferrals = all
            .iter()
            .filter(|event| event.event_type == "session.execution_adoption_deferred")
            .count();
        assert_eq!(deferrals, 1, "deferral event must be recorded once");

        // Releasing the lease (a clean shutdown handoff) lets the next scan
        // adopt immediately; this also terminalizes the execution before the
        // test releases TEST_LOCK.
        store
            .release_stdout_owner(&execution_id, "other-control-plane")
            .await
            .expect("release lease");
        runtime
            .run_orphan_adoption_scan(&mut state, Some(PRE_SANDBOX_ORPHAN_GRACE))
            .await;
        wait_for_event(&store, &thread_key, "session.execution_completed").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn terminal_paths_stop_stdout_owner_renewers() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let runtime = runtime_with(&store, backend);

        let completed_thread =
            ThreadKey::parse(format!("test:renewer-completed-{}", uuid::Uuid::new_v4())).unwrap();
        let completed_execution = orphaned_execution(
            &store,
            &completed_thread,
            Some("sbx-renewer-completed"),
            true,
        )
        .await;
        runtime
            .claim_stdout_owner(&completed_execution)
            .await
            .expect("claim completed execution");
        assert!(
            record_terminal_output(
                &runtime.context(),
                &completed_thread,
                "sbx-renewer-completed",
                &completed_execution,
                TerminalOutput::Completed {
                    reason: "test",
                    result_text: Some("done".to_owned()),
                },
            )
            .await
            .expect("record completed terminal")
        );
        assert!(
            !runtime
                .stdout_owner_renewals
                .contains_key(&completed_execution)
        );

        let max_thread =
            ThreadKey::parse(format!("test:renewer-max-{}", uuid::Uuid::new_v4())).unwrap();
        let max_execution =
            orphaned_execution(&store, &max_thread, Some("sbx-renewer-max"), true).await;
        runtime
            .claim_stdout_owner(&max_execution)
            .await
            .expect("claim max-duration execution");
        record_max_duration_failure(
            &runtime.context(),
            &max_thread,
            &max_execution,
            Duration::from_millis(5),
            None,
        )
        .await
        .expect("record max-duration terminal");
        assert!(!runtime.stdout_owner_renewals.contains_key(&max_execution));

        let startup_thread =
            ThreadKey::parse(format!("test:renewer-startup-{}", uuid::Uuid::new_v4())).unwrap();
        let startup_execution =
            orphaned_execution(&store, &startup_thread, Some("sbx-renewer-startup"), true).await;
        runtime
            .claim_stdout_owner(&startup_execution)
            .await
            .expect("claim startup-failure execution");
        runtime
            .record_execution_failure(
                &startup_thread,
                &startup_execution,
                &SessionRuntimeError::BadRequest("test startup failure".to_owned()),
            )
            .await;
        assert!(
            !runtime
                .stdout_owner_renewals
                .contains_key(&startup_execution)
        );

        for (thread_key, execution_id) in [
            (&completed_thread, &completed_execution),
            (&max_thread, &max_execution),
            (&startup_thread, &startup_execution),
        ] {
            assert!(
                store
                    .list_events_after(thread_key, 0, Some(execution_id), 100)
                    .await
                    .expect("read terminal events")
                    .iter()
                    .any(|event| is_terminal_execution_event(&event.event_type)),
                "each terminal status must commit its terminal event"
            );
        }
        assert!(runtime.stdout_owner_renewals.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_handoff_releases_owned_leases() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:handoff-{}", uuid::Uuid::new_v4())).unwrap();
        let execution_id = orphaned_execution(&store, &thread_key, Some("sbx-mock"), true).await;
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let runtime = runtime_with(&store, backend);
        runtime
            .claim_stdout_owner(&execution_id)
            .await
            .expect("claim as this control plane");
        assert!(runtime.stdout_owner_renewals.contains_key(&execution_id));

        runtime.handoff_owned_executions(Duration::ZERO).await;

        assert!(
            runtime.stdout_owner_renewals.is_empty(),
            "handoff must stop renewers before releasing leases"
        );
        wait_for_event(&store, &thread_key, "session.stdout_owner_released").await;
        // The lease is immediately claimable by a peer control plane; without
        // the handoff it would only expire after the lease TTL.
        assert!(
            store
                .claim_stdout_owner(&execution_id, "peer-control-plane", Duration::from_secs(5))
                .await
                .expect("peer claims released lease")
        );
        store
            .fail_execution_if_active(&execution_id, "test cleanup")
            .await
            .expect("terminalize execution");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_handoff_waits_for_executions_to_finish() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:handoff-wait-{}", uuid::Uuid::new_v4())).unwrap();
        let execution_id = orphaned_execution(&store, &thread_key, Some("sbx-mock"), true).await;
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let runtime = runtime_with(&store, backend);
        runtime
            .claim_stdout_owner(&execution_id)
            .await
            .expect("claim as this control plane");

        // The execution finishes while the drain is waiting; no lease should
        // be released and no handoff event recorded.
        let completer_store = store.clone();
        let completer_id = execution_id.clone();
        let completer = tokio::spawn(async move {
            sleep(Duration::from_millis(300)).await;
            completer_store
                .complete_execution_if_active(&completer_id)
                .await
                .expect("complete execution")
        });
        let handoff_runtime = runtime.clone();
        let handoff = tokio::spawn(async move {
            handoff_runtime
                .handoff_owned_executions(Duration::from_secs(5))
                .await;
        });
        sleep(Duration::from_millis(100)).await;
        assert!(
            runtime.stdout_owner_renewals.contains_key(&execution_id),
            "shutdown drain must keep renewing while the execution can still finish"
        );
        handoff.await.expect("join shutdown handoff");
        let completed = completer.await.expect("completer task");
        assert!(
            completed.is_some(),
            "the completer, not the handoff, must terminalize the execution"
        );

        let all = events(&store, &thread_key).await;
        assert!(
            all.iter()
                .all(|event| event.event_type != "session.stdout_owner_released"),
            "finished execution must not be handed off"
        );
        assert!(
            runtime.stdout_owner_renewals.is_empty(),
            "shutdown must stop renewers after the drain finishes"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_fences_new_stdout_claims() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let runtime = runtime_with(&store, backend.clone());

        // Nothing owned: the handoff returns immediately but still flips
        // the shutdown fence.
        runtime.handoff_owned_executions(Duration::ZERO).await;

        let thread_key =
            ThreadKey::parse(format!("test:handoff-fence-{}", uuid::Uuid::new_v4())).unwrap();
        let execution_id = orphaned_execution(&store, &thread_key, Some("sbx-mock"), true).await;
        let error = runtime
            .claim_stdout_owner(&execution_id)
            .await
            .expect_err("claims after shutdown must be rejected");
        assert!(
            matches!(error, SessionRuntimeError::ShuttingDown),
            "unexpected error: {error}"
        );
        let error = runtime
            .claim_expired_stdout_owner(&execution_id)
            .await
            .expect_err("adoption claims after shutdown must be rejected");
        assert!(matches!(error, SessionRuntimeError::ShuttingDown));

        backend.set_recorded_output(completed_output_lines(
            "A shutting-down runtime must not adopt this.",
        ));
        runtime.adopt_orphaned_executions().await;
        let all = events(&store, &thread_key).await;
        assert!(
            all.iter().all(|event| {
                event.event_type != "session.execution_adopted"
                    && event.event_type != "session.execution_completed"
                    && event.event_type != "session.execution_failed"
            }),
            "shutdown must fence adoption scans and orphan-failure claims"
        );
        assert!(
            store
                .active_execution_for_thread(&thread_key)
                .await
                .expect("load shutdown-fenced execution")
                .is_some()
        );
        store
            .fail_execution_if_active(&execution_id, "test cleanup")
            .await
            .expect("terminalize execution");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_handoff_fences_a_claim_already_waiting_in_the_database() {
        let _serial = TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:handoff-claim-race-{}", uuid::Uuid::new_v4())).unwrap();
        let execution_id = orphaned_execution(&store, &thread_key, Some("sbx-mock"), true).await;
        let backend = Arc::new(MockBackend::new(SandboxStatus::Running, Vec::new()));
        let runtime = runtime_with(&store, backend);

        let mut row_lock = store
            .pool()
            .begin()
            .await
            .expect("begin execution row lock");
        sqlx::query(
            "select execution_id from session_executions where execution_id = $1 for update",
        )
        .bind(&execution_id)
        .fetch_one(&mut *row_lock)
        .await
        .expect("lock execution row");

        let claim_runtime = runtime.clone();
        let claim_execution_id = execution_id.clone();
        let claim =
            tokio::spawn(
                async move { claim_runtime.claim_stdout_owner(&claim_execution_id).await },
            );
        timeout(
            Duration::from_secs(2),
            runtime.stdout_owner_claim_db_started.notified(),
        )
        .await
        .expect("claim must reach its database update");

        let handoff_runtime = runtime.clone();
        let handoff = tokio::spawn(async move {
            handoff_runtime
                .handoff_owned_executions(Duration::ZERO)
                .await;
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        while !runtime.shutting_down.load(Ordering::SeqCst) {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for shutdown fence"
            );
            tokio::task::yield_now().await;
        }
        row_lock
            .rollback()
            .await
            .expect("release execution row lock");

        let error = claim
            .await
            .expect("join in-flight claim")
            .expect_err("in-flight claim must yield to shutdown");
        assert!(matches!(error, SessionRuntimeError::ShuttingDown));
        handoff.await.expect("join handoff");
        assert_eq!(
            store
                .count_executions_with_stdout_owner(&runtime.stdout_owner_id)
                .await
                .expect("count post-handoff owners"),
            0,
            "no claim may commit after handoff releases this runtime's leases"
        );
        assert!(
            store
                .claim_stdout_owner(&execution_id, "peer-control-plane", Duration::from_secs(5),)
                .await
                .expect("peer claims after handoff")
        );
        store
            .fail_execution_if_active(&execution_id, "test cleanup")
            .await
            .expect("terminalize claim-race execution");
    }
}
