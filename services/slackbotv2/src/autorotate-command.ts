import { createHmac, timingSafeEqual } from 'node:crypto'
import type { Logger } from 'chat'
import type { JsonObject, SlackbotV2Fetch } from './types'
import { isJsonObject, stringValue } from './utils'

const AUTOROTATE_COMMAND = '/autorotate'
const MAX_SLACK_REQUEST_AGE_SECONDS = 5 * 60
const MAX_RESPONSE_BYTES = 64 * 1024
const DEFAULT_REQUEST_TIMEOUT_MS = 2_000
const DEFAULT_POLL_INTERVAL_MS = 5_000
const DEFAULT_ENROLLMENT_LIFETIME_MS = 15 * 60_000
const DEFAULT_TRACE_CONSENT_LIFETIME_MS = 60 * 60_000
const MAX_TRACE_CONSENT_LIFETIME_MS = 24 * 60 * 60_000
const MAINTENANCE_MUTATION_MAX_RETRIES = 3
const MAINTENANCE_MUTATION_DEADLINE_MS = 8_000
const MAINTENANCE_RETRY_BASE_DELAY_MS = 35
const MAX_STATUS_TEXT_CHARS = 35_000
const START_ENROLLMENT_FIELDS = [
  'enrollment_id',
  'action',
  'account_label',
  'verification_url',
  'user_code',
  'expires_at',
  'status'
] as const
const ENROLLMENT_STATUS_FIELDS = [
  'enrollment_id',
  'status',
  'expires_at',
  'account',
  'error_code'
] as const
const ENROLLMENT_ACCOUNT_FIELDS = ['label', 'email', 'status'] as const
const ACCOUNT_FIELDS = [
  'label',
  'email',
  'status',
  'limited_until',
  'login_required',
  'reconciliation_required',
  'active_writer',
  'availability',
  'unusable_reason',
  'limits_observed_at',
  'primary',
  'secondary',
  'reset_credits',
  'next_available_at'
] as const
const REQUIRED_ACCOUNT_FIELDS = ACCOUNT_FIELDS.filter(field => field !== 'reset_credits')
const RATE_LIMIT_FIELDS = [
  'used_percent',
  'resets_at',
  'window_minutes'
] as const
const RESET_CREDITS_FIELDS = ['available_count', 'credits'] as const
const RESET_CREDIT_FIELDS = ['expires_at'] as const
const ENROLLMENT_ID_PATTERN = /^[A-Za-z0-9_-]{8,128}$/
const SLACK_MEMBER_ID_PATTERN = /^[UW][A-Z0-9]+$/i
const SLACK_TEAM_ID_PATTERN = /^T[A-Z0-9]+$/i
const OPENAI_DEVICE_LOGIN_HOSTS = new Set(['auth.openai.com'])

type EnrollmentAction = 'add' | 'relogin'
type EnrollmentStatus = 'pending' | 'importing' | 'completed' | 'failed' | 'cancelled' | 'expired'
type EnrollmentAccount = {
  email: string | null
  label: string
  status: AutorotateAccount['status']
}

type StartEnrollmentResponse = {
  enrollment_id: string
  action: EnrollmentAction
  account_label?: string
  verification_url?: string
  user_code?: string
  expires_at: string
  status: 'pending' | 'importing'
}

type EnrollmentStatusResponse = {
  enrollment_id: string
  expires_at: string
  status: EnrollmentStatus
  account: EnrollmentAccount | null
  error_code: string | null
}

type AutorotateAccount = {
  active_writer: boolean
  availability: 'available' | 'in_use' | 'rate_limited' | 'login_required' | 'reconciliation_required' | 'disabled'
  label: string
  email: string | null
  status: 'enabled' | 'disabled' | 'dead'
  limited_until: string | null
  limits_observed_at: string | null
  login_required: boolean
  next_available_at: string | null
  primary: RateLimitWindow | null
  reconciliation_required: boolean
  reset_credits: ResetCredits | null
  secondary: RateLimitWindow | null
  unusable_reason: AccountUnusableReason | null
}

type AccountUnusableReason =
  | 'refresh_token_expired'
  | 'refresh_token_reused'
  | 'refresh_token_revoked'
  | 'account_mismatch'
  | 'login_required'
  | 'reconciliation_required'
  | 'operator_reported'

type RateLimitWindow = {
  resets_at: string | null
  used_percent: number
  window_minutes: number | null
}

type ResetCredit = {
  expires_at: string | null
}

type ResetCredits = {
  available_count: number
  credits: ResetCredit[]
}

type ActiveEnrollmentState = 'starting' | 'monitoring' | 'cancel_requested' | 'cancelling'

type ActiveEnrollment = {
  brokerStatus: 'pending' | 'importing'
  enrollmentId?: string
  expiresAtMs: number
  owner: string
  responseUrl: string
  state: ActiveEnrollmentState
}

type CreateEnrollmentInput = {
  action: 'relogin'
  owner: string
}

type AutorotateSlackOptions = {
  apiKey?: string
  apiUrl: string
  brokerUrl?: string
  fetch?: SlackbotV2Fetch
  logger: Logger
  operatorSlackTeamIds?: readonly string[]
  operatorSlackUserIds?: readonly string[]
  operatorToken?: string
  controlToken?: string
  pollIntervalMs?: number
  requestTimeoutMs?: number
  responseUrlHosts?: readonly string[]
  signingSecret: string
}

type FleetSession = {
  account_email: string | null
  account_label: string
  client_version: string | null
  consumer_fingerprint: string
  created_at: string
  expires_at: string
  last_heartbeat_at: string | null
}

type FleetReport = {
  active_sessions: FleetSession[]
  generated_at: string
  terminal_leases: TerminalLeaseCounts
}

type TerminalLeaseCounts = {
  clean: number
  expired: number
  expired_unreleased: number
  other: number
  total: number
  unknown_legacy: number
  unusable: number
}

type MaintenanceStatus = {
  active_enrollments: number
  active_leases: number
  active_quota_probes: number
  changed_at: string
  control_epoch: number
  drain_clean_terminals: number
  drain_completion: 'pending' | 'drained' | 'drained_with_unclean_sessions' | null
  drain_expired_terminals: number
  drain_other_terminals: number
  mode: 'draining' | 'serving'
  quiescent: boolean
  reason_code: string | null
}

type MaintenanceCommand =
  | { kind: 'maintenance_status' }
  | { kind: 'maintenance_drain' }
  | { kind: 'maintenance_resume' }

type MaintenanceOperation = 'drain' | 'resume'

type SlackTraceConsent = {
  drain_pending: boolean
  enabled: boolean
  expires_at: string | null
  revision: number
  user_id: string
  workspace_id: string
}

type AutorotateSlackCommandHandler = {
  handle(request: Request, waitUntil: (promise: Promise<unknown>) => void): Promise<Response | null>
}

class AutorotateError extends Error {
  constructor(
    message: string,
    readonly status?: number,
    readonly code?: string
  ) {
    super(message)
  }
}

class AutorotateClient {
  private readonly baseUrl: URL
  private readonly fetchFn: SlackbotV2Fetch
  private readonly requestTimeoutMs: number

  constructor(
    brokerUrl: string,
    private readonly operatorToken: string,
    fetchFn: SlackbotV2Fetch,
    requestTimeoutMs: number
  ) {
    this.baseUrl = parseBrokerUrl(brokerUrl)
    this.fetchFn = fetchFn
    this.requestTimeoutMs = requestTimeoutMs
  }

  async createEnrollment(input: CreateEnrollmentInput): Promise<StartEnrollmentResponse> {
    const payload = await this.request(
      'POST',
      'v1/operator/enrollments',
      this.operatorToken,
      input
    )
    return validateStartEnrollment(selectFields(payload, START_ENROLLMENT_FIELDS))
  }

  async enrollment(enrollmentId: string): Promise<EnrollmentStatusResponse> {
    if (!ENROLLMENT_ID_PATTERN.test(enrollmentId)) {
      throw new AutorotateError('invalid enrollment id')
    }
    const payload = await this.request(
      'GET',
      `v1/operator/enrollments/${encodeURIComponent(enrollmentId)}`,
      this.operatorToken
    )
    return validateEnrollmentStatus(selectFields(payload, ENROLLMENT_STATUS_FIELDS))
  }

  async activeEnrollment(owner: string): Promise<StartEnrollmentResponse | null> {
    try {
      const payload = await this.request(
        'GET',
        `v1/operator/enrollments?owner=${encodeURIComponent(owner)}`,
        this.operatorToken
      )
      return validateStartEnrollment(selectFields(payload, START_ENROLLMENT_FIELDS))
    } catch (error) {
      if (error instanceof AutorotateError && error.status === 404) return null
      throw error
    }
  }

  async accounts(): Promise<AutorotateAccount[]> {
    const payload = await this.request(
      'GET',
      'v1/operator/accounts',
      this.operatorToken
    )
    if (!Array.isArray(payload.accounts)) {
      throw new AutorotateError('Autorotate returned invalid accounts')
    }
    return payload.accounts.map(account => {
      if (!isJsonObject(account)) {
        throw new AutorotateError('Autorotate returned invalid accounts')
      }
      return validateAccount(selectFields(account, ACCOUNT_FIELDS))
    })
  }

  async cancelEnrollment(enrollmentId: string): Promise<EnrollmentStatusResponse | null> {
    if (!ENROLLMENT_ID_PATTERN.test(enrollmentId)) {
      throw new AutorotateError('invalid enrollment id')
    }
    const payload = await this.request(
      'DELETE',
      `v1/operator/enrollments/${encodeURIComponent(enrollmentId)}`,
      this.operatorToken,
      undefined,
      true
    )
    return Object.keys(payload).length === 0
      ? null
      : validateEnrollmentStatus(selectFields(payload, ENROLLMENT_STATUS_FIELDS))
  }

  private async request(
    method: string,
    path: string,
    token: string,
    body?: JsonObject,
    allowEmpty = false
  ): Promise<JsonObject> {
    if (!token) throw new AutorotateError('Autorotate credential is not configured')
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), this.requestTimeoutMs)
    try {
      const response = await this.fetchFn(new URL(path, this.baseUrl), {
        method,
        headers: {
          accept: 'application/json',
          authorization: `Bearer ${token}`,
          ...(body ? { 'content-type': 'application/json' } : {})
        },
        body: body ? JSON.stringify(body) : undefined,
        redirect: 'error',
        signal: controller.signal
      })
      const text = await response.text()
      if (text.length > MAX_RESPONSE_BYTES) {
        throw new AutorotateError('Autorotate response was too large')
      }
      if (allowEmpty && response.ok && text.length === 0) return {}
      const payload = parseJsonObject(text)
      if (!response.ok) {
        const error = isJsonObject(payload.error) ? payload.error : {}
        throw new AutorotateError(
          `Autorotate returned HTTP ${response.status}`,
          response.status,
          stringValue(error.code)
        )
      }
      return payload
    } catch (error) {
      if (error instanceof AutorotateError) throw error
      throw new AutorotateError('Autorotate request failed')
    } finally {
      clearTimeout(timeout)
    }
  }
}

/** The control credential is intentionally isolated from account and runner clients. */
class AutorotateControlClient {
  private readonly baseUrl: URL
  private readonly fetchFn: SlackbotV2Fetch
  private readonly requestTimeoutMs: number

  constructor(
    brokerUrl: string,
    private readonly controlToken: string,
    fetchFn: SlackbotV2Fetch,
    requestTimeoutMs: number
  ) {
    this.baseUrl = parseBrokerUrl(brokerUrl)
    this.fetchFn = fetchFn
    this.requestTimeoutMs = requestTimeoutMs
  }

  async fleet(): Promise<FleetReport> {
    return validateFleetReport(await this.requestOnce('GET', 'v1/fleet'))
  }

  async status(): Promise<MaintenanceStatus> {
    return validateMaintenanceStatus(await this.requestOnce('GET', 'v1/maintenance'))
  }

  /** Reads the exact durable response for one control request without rebuilding its body. */
  async mutationResult(
    requestId: string,
    expectedOperation: MaintenanceOperation,
    deadline?: number
  ): Promise<MaintenanceStatus | null> {
    try {
      return validateMaintenanceOperationResult(
        await this.requestOnce(
          'GET',
          `v1/maintenance/requests/${encodeURIComponent(requestId)}`,
          undefined,
          undefined,
          deadline
        ),
        expectedOperation
      )
    } catch (error) {
      if (error instanceof AutorotateError && error.status === 404) return null
      throw error
    }
  }

  async mutate(
    action: 'drain' | 'resume',
    body: JsonObject,
    requestId: string
  ): Promise<MaintenanceStatus> {
    const deadline = Date.now() + MAINTENANCE_MUTATION_DEADLINE_MS
    let lastError: unknown
    for (let attempt = 0; attempt <= MAINTENANCE_MUTATION_MAX_RETRIES; attempt += 1) {
      try {
        return validateMaintenanceStatus(
          await this.requestOnce('POST', `v1/maintenance/${action}`, body, requestId, deadline)
        )
      } catch (error) {
        lastError = error
        if (maintenanceRequestHashConflict(error)) {
          const stored = await this.lookupMutationResultUntilDeadline(action, requestId, deadline)
          if (stored) return stored
          break
        }
        if (retryableMaintenanceMutationError(error)) {
          try {
            const stored = await this.mutationResult(requestId, action, deadline)
            if (stored) return stored
          } catch (lookupError) {
            if (!retryableMaintenanceMutationError(lookupError)) throw lookupError
          }
        }
        if (
          !retryableMaintenanceMutationError(error)
          || attempt >= MAINTENANCE_MUTATION_MAX_RETRIES
          || Date.now() >= deadline
        ) break
        await delay(jitteredMaintenanceDelay(attempt, deadline))
      }
    }
    if (retryableMaintenanceMutationError(lastError)) {
      const stored = await this.lookupMutationResultUntilDeadline(action, requestId, deadline)
      if (stored) return stored
    }
    throw lastError instanceof Error ? lastError : new AutorotateError('maintenance request failed')
  }

  private async lookupMutationResultUntilDeadline(
    action: MaintenanceOperation,
    requestId: string,
    deadline: number
  ): Promise<MaintenanceStatus | null> {
    let attempt = 0
    while (Date.now() < deadline) {
      try {
        const stored = await this.mutationResult(requestId, action, deadline)
        if (stored) return stored
      } catch (error) {
        if (!retryableMaintenanceMutationError(error)) throw error
      }
      await delay(jitteredMaintenanceDelay(attempt, deadline))
      attempt += 1
    }
    return null
  }

  private async requestOnce(
    method: 'GET' | 'POST',
    path: string,
    body?: JsonObject,
    requestId?: string,
    deadline?: number
  ): Promise<JsonObject> {
    if (!this.controlToken) throw new AutorotateError('Autorotate control credential is not configured')
    const timeoutMs = deadline === undefined
      ? this.requestTimeoutMs
      : Math.min(this.requestTimeoutMs, deadline - Date.now())
    if (timeoutMs <= 0) throw new AutorotateError('Autorotate control request deadline elapsed')
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), timeoutMs)
    try {
      const response = await this.fetchFn(new URL(path, this.baseUrl), {
        method,
        headers: {
          accept: 'application/json',
          authorization: `Bearer ${this.controlToken}`,
          'cache-control': 'no-store',
          ...(body ? { 'content-type': 'application/json' } : {}),
          ...(requestId ? { 'x-request-id': requestId } : {})
        },
        body: body ? JSON.stringify(body) : undefined,
        cache: 'no-store',
        redirect: 'error',
        signal: controller.signal
      })
      const text = await response.text()
      if (text.length > MAX_RESPONSE_BYTES) {
        throw new AutorotateError('Autorotate control response was too large')
      }
      if (!response.ok) {
        const payload = tryParseJsonObject(text)
        const error = payload && isJsonObject(payload.error) ? payload.error : {}
        throw new AutorotateError(
          `Autorotate control returned HTTP ${response.status}`,
          response.status,
          stringValue(error.code)
        )
      }
      return parseJsonObject(text)
    } catch (error) {
      if (error instanceof AutorotateError) throw error
      throw new AutorotateError('Autorotate control request was interrupted')
    } finally {
      clearTimeout(timeout)
    }
  }
}

class SlackTraceConsentClient {
  private readonly baseUrl: URL
  private readonly fetchFn: SlackbotV2Fetch
  private readonly requestTimeoutMs: number

  constructor(
    apiUrl: string,
    private readonly apiKey: string,
    fetchFn: SlackbotV2Fetch,
    requestTimeoutMs: number
  ) {
    this.baseUrl = parseApiUrl(apiUrl)
    this.fetchFn = fetchFn
    this.requestTimeoutMs = requestTimeoutMs
  }

  async consent(workspaceId: string, userId: string): Promise<SlackTraceConsent> {
    return this.request('GET', workspaceId, userId)
  }

  async enable(
    workspaceId: string,
    userId: string,
    expiresAt: string
  ): Promise<SlackTraceConsent> {
    return this.request('PUT', workspaceId, userId, { data: { expires_at: expiresAt } })
  }

  async disable(
    workspaceId: string,
    userId: string,
    idempotencyKey: string
  ): Promise<SlackTraceConsent> {
    return this.request('DELETE', workspaceId, userId, undefined, idempotencyKey)
  }

  private async request(
    method: 'DELETE' | 'GET' | 'PUT',
    workspaceId: string,
    userId: string,
    body?: JsonObject,
    idempotencyKey?: string
  ): Promise<SlackTraceConsent> {
    if (!this.apiKey) throw new AutorotateError('Centaur API credential is not configured')
    return this.requestOnce(method, workspaceId, userId, body, idempotencyKey)
  }

  private async requestOnce(
    method: 'DELETE' | 'GET' | 'PUT',
    workspaceId: string,
    userId: string,
    body?: JsonObject,
    idempotencyKey?: string
  ): Promise<SlackTraceConsent> {
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), this.requestTimeoutMs)
    try {
      const url = new URL(
        `/api/v1/slack_trace_consents/${encodeURIComponent(workspaceId)}/${encodeURIComponent(userId)}`,
        this.baseUrl
      )
      const response = await this.fetchFn(url, {
        method,
        headers: {
          accept: 'application/json',
          authorization: `Bearer ${this.apiKey}`,
          ...(body ? { 'content-type': 'application/json' } : {}),
          ...(idempotencyKey ? { 'idempotency-key': idempotencyKey } : {})
        },
        body: body ? JSON.stringify(body) : undefined,
        cache: 'no-store',
        redirect: 'error',
        signal: controller.signal
      })
      const text = await response.text()
      if (text.length > MAX_RESPONSE_BYTES) {
        throw new AutorotateError('Centaur API response was too large')
      }
      const payload = parseJsonObject(text)
      if (!response.ok) {
        const error = isJsonObject(payload.error) ? payload.error : {}
        throw new AutorotateError(
          `Centaur API returned HTTP ${response.status}`,
          response.status,
          stringValue(error.code)
        )
      }
      return validateTraceConsent(payload, workspaceId, userId)
    } catch (error) {
      if (error instanceof AutorotateError) throw error
      throw new AutorotateError('Centaur API request failed')
    } finally {
      clearTimeout(timeout)
    }
  }
}

export function createAutorotateSlackCommandHandler(
  options: AutorotateSlackOptions
): AutorotateSlackCommandHandler {
  const activeEnrollments = new Map<string, ActiveEnrollment>()
  const fetchFn = options.fetch ?? fetch
  const requestTimeoutMs = options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS
  const pollIntervalMs = options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS
  const brokerUrl = options.brokerUrl?.trim()
  const client = brokerUrl
    ? new AutorotateClient(
        brokerUrl,
        options.operatorToken?.trim() ?? '',
        fetchFn,
        requestTimeoutMs
      )
    : null
  const controlClient = brokerUrl
    ? new AutorotateControlClient(
        brokerUrl,
        options.controlToken?.trim() ?? '',
        fetchFn,
        requestTimeoutMs
      )
    : null
  const traceClient = new SlackTraceConsentClient(
    options.apiUrl,
    options.apiKey?.trim() ?? process.env.SLACKBOT_API_KEY?.trim() ?? '',
    fetchFn,
    requestTimeoutMs
  )

  return {
    async handle(request, waitUntil) {
      const rawBody = await request.clone().text()
      const form = new URLSearchParams(rawBody)
      if (form.get('command') !== AUTOROTATE_COMMAND) return null

      if (!validSlackSignature(request.headers, rawBody, options.signingSecret)) {
        return new Response('invalid Slack signature', { status: 401 })
      }
      const requestTimestampSeconds = verifiedSlackRequestTimestamp(request.headers)
      if (requestTimestampSeconds === null) {
        return new Response('invalid Slack signature', { status: 401 })
      }

      const teamId = form.get('team_id')?.trim() ?? ''
      const userId = form.get('user_id')?.trim() ?? ''
      if (!authorizedWorkspace(options, teamId, userId)) {
        return ephemeralResponse('You are not authorized to operate the Codex account pool.')
      }
      const command = parseCommand(form.get('text') ?? '')
      const actorKey = `${teamId}:${userId}`
      const owner = `slack:${teamId}:${userId}`
      if (command.kind === 'help') return ephemeralResponse(helpText())
      if (isTraceCommand(command)) {
        if (command.kind === 'trace_off') {
          try {
            return ephemeralResponse(formatTraceConsent(await traceClient.disable(
              teamId,
              userId,
              traceOffIdempotencyKey(request.headers, options.signingSecret)
            )))
          } catch (error) {
            safeCommandWarning(options.logger, 'slackbotv2_trace_consent_revoke_failed', error)
            return ephemeralResponse(traceConsentFailure(command.kind))
          }
        }
        try {
          return ephemeralResponse(await traceConsentResponse({
            command,
            client: traceClient,
            requestTimestampSeconds,
            userId,
            workspaceId: teamId
          }))
        } catch (error) {
          safeCommandWarning(options.logger, 'slackbotv2_trace_consent_failed', error)
          return ephemeralResponse(traceConsentFailure(command.kind))
        }
      }
      if (!client) {
        return ephemeralResponse('Autorotate is not configured for this deployment.')
      }
      if (isMaintenanceCommand(command)) {
        const responseUrl = safeResponseUrl(form.get('response_url'), options)
        if (!responseUrl) {
          return ephemeralResponse('Slack did not provide a safe ephemeral response channel.')
        }
        if (!controlClient) {
          return ephemeralResponse('Autorotate maintenance is not configured for this deployment.')
        }
        if (isMaintenanceMutation(command) && !authorizedMaintenanceOperator(options, teamId, userId)) {
          return ephemeralResponse('You are not authorized to change Autorotate maintenance.')
        }
        waitUntil(respondWithMaintenance({
          command,
          controlClient,
          fetchFn,
          logger: options.logger,
          requestId: maintenanceRequestId(request.headers, options.signingSecret, command.kind),
          responseUrl,
          timeoutMs: requestTimeoutMs
        }))
        return ephemeralResponse(maintenanceAcknowledgement(command.kind))
      }
      if (command.kind === 'fleet') {
        const responseUrl = safeResponseUrl(form.get('response_url'), options)
        if (!responseUrl) {
          return ephemeralResponse('Slack did not provide a safe ephemeral response channel.')
        }
        if (!controlClient) {
          return ephemeralResponse('Autorotate fleet is not configured for this deployment.')
        }
        waitUntil(respondWithFleet(controlClient, responseUrl, options.logger, fetchFn, requestTimeoutMs))
        return ephemeralResponse('Checking redacted Codex fleet status…')
      }
      if (command.kind === 'status') {
        const responseUrl = safeResponseUrl(form.get('response_url'), options)
        if (!responseUrl) {
          return ephemeralResponse('Slack did not provide a safe ephemeral response channel.')
        }
        waitUntil(respondWithStatus(client, responseUrl, options, fetchFn))
        return ephemeralResponse('Checking Codex account status…')
      }

      if (command.kind === 'login') {
        const responseUrl = safeResponseUrl(form.get('response_url'), options)
        if (!responseUrl) {
          return ephemeralResponse('Slack did not provide a safe ephemeral response channel.')
        }
        const existing = activeEnrollments.get(actorKey)
        if (existing && existing.expiresAtMs <= Date.now()) {
          activeEnrollments.delete(actorKey)
        } else if (existing) {
          return ephemeralResponse(
            'A Codex login is already active. Wait for it to finish, or try again after it expires.'
          )
        }
        const active: ActiveEnrollment = {
          brokerStatus: 'pending',
          expiresAtMs: Date.now() + DEFAULT_ENROLLMENT_LIFETIME_MS,
          owner,
          responseUrl,
          state: 'starting'
        }
        activeEnrollments.set(actorKey, active)
        waitUntil(
          startAndMonitorEnrollment({
            active,
            activeEnrollments,
            actorKey,
            client,
            fetchFn,
            options,
            pollIntervalMs,
            responseUrl
          })
        )
        return ephemeralResponse('Starting Codex device login…')
      }

      const responseUrl = safeResponseUrl(form.get('response_url'), options)
      if (!responseUrl) {
        return ephemeralResponse('Slack did not provide a safe ephemeral response channel.')
      }
      const active = activeEnrollments.get(actorKey)
      if (command.kind === 'login_status') {
        if (active?.state === 'starting') {
          return ephemeralResponse('Your Codex device login is still starting.')
        }
        if (active?.state === 'cancel_requested' || active?.state === 'cancelling') {
          return ephemeralResponse('Your Codex device login is being cancelled.')
        }
        waitUntil(
          respondWithEnrollmentStatus({
            active,
            activeEnrollments,
            actorKey,
            client,
            fetchFn,
            options,
            owner,
            pollIntervalMs,
            responseUrl
          })
        )
        return ephemeralResponse('Checking your Codex login…')
      }

      if (active?.state === 'starting') {
        active.state = 'cancel_requested'
        return ephemeralResponse('Cancelling your Codex login as soon as it starts…')
      }
      if (active?.state === 'cancel_requested' || active?.state === 'cancelling') {
        return ephemeralResponse('Your Codex login is already being cancelled.')
      }
      if (active) active.state = 'cancelling'
      waitUntil(
        cancelEnrollment({
          active,
          activeEnrollments,
          actorKey,
          client,
          fetchFn,
          options,
          owner,
          pollIntervalMs,
          responseUrl
        })
      )
      return ephemeralResponse('Cancelling your Codex login…')
    }
  }
}

async function respondWithStatus(
  client: AutorotateClient,
  responseUrl: string,
  options: AutorotateSlackOptions,
  fetchFn: SlackbotV2Fetch
): Promise<void> {
  try {
    const accounts = await client.accounts()
    await postSlackResponse(
      fetchFn,
      responseUrl,
      formatStatus(accounts),
      options.requestTimeoutMs
    )
  } catch (error) {
    safeCommandWarning(options.logger, 'slackbotv2_autorotate_status_failed', error)
    await postSlackResponse(
      fetchFn,
      responseUrl,
      'Codex account status is temporarily unavailable.',
      options.requestTimeoutMs
    )
  }
}

async function respondWithFleet(
  client: AutorotateControlClient,
  responseUrl: string,
  logger: Logger,
  fetchFn: SlackbotV2Fetch,
  timeoutMs: number
): Promise<void> {
  try {
    await postSlackResponse(fetchFn, responseUrl, formatFleet(await client.fleet()), timeoutMs)
  } catch (error) {
    safeCommandWarning(logger, 'slackbotv2_autorotate_fleet_failed', error)
    await postSlackResponse(
      fetchFn,
      responseUrl,
      'Redacted Codex fleet status is temporarily unavailable.',
      timeoutMs
    )
  }
}

async function respondWithMaintenance(input: {
  command: MaintenanceCommand
  controlClient: AutorotateControlClient
  fetchFn: SlackbotV2Fetch
  logger: Logger
  requestId: string
  responseUrl: string
  timeoutMs: number
}): Promise<void> {
  let text: string
  try {
    if (input.command.kind === 'maintenance_status') {
      text = formatMaintenanceStatus(await input.controlClient.status())
    } else {
      const action: MaintenanceOperation = input.command.kind === 'maintenance_drain' ? 'drain' : 'resume'
      const stored = await input.controlClient.mutationResult(input.requestId, action)
      if (stored) {
        text = formatMaintenanceStatus(stored)
      } else {
        const before = await input.controlClient.status()
        const body = action === 'drain'
          ? { expected_control_epoch: before.control_epoch, reason_code: 'slack_maintenance' }
          : { expected_drain_epoch: before.control_epoch }
        text = formatMaintenanceStatus(await input.controlClient.mutate(
          action,
          body,
          input.requestId
        ))
      }
    }
  } catch (error) {
    safeCommandWarning(input.logger, 'slackbotv2_autorotate_maintenance_failed', error)
    text = 'Autorotate maintenance status is temporarily unavailable.'
  }
  try {
    await postSlackResponse(input.fetchFn, input.responseUrl, text, input.timeoutMs)
  } catch (error) {
    safeCommandWarning(input.logger, 'slackbotv2_autorotate_maintenance_response_failed', error)
  }
}

async function traceConsentResponse(input: {
  command: Extract<ParsedCommand, { kind: 'trace_on' | 'trace_status' }>
  client: SlackTraceConsentClient
  requestTimestampSeconds: number
  userId: string
  workspaceId: string
}): Promise<string> {
  const consent = input.command.kind === 'trace_status'
    ? await input.client.consent(input.workspaceId, input.userId)
    : input.command.kind === 'trace_on'
      ? await input.client.enable(
          input.workspaceId,
          input.userId,
          new Date(input.requestTimestampSeconds * 1000 + input.command.durationMs).toISOString()
        )
      : unreachableTraceCommand(input.command)
  return formatTraceConsent(consent)
}

function unreachableTraceCommand(command: never): never {
  throw new AutorotateError(`unsupported trace command: ${command}`)
}

async function startAndMonitorEnrollment(input: {
  active: ActiveEnrollment
  activeEnrollments: Map<string, ActiveEnrollment>
  actorKey: string
  client: AutorotateClient
  fetchFn: SlackbotV2Fetch
  options: AutorotateSlackOptions
  pollIntervalMs: number
  responseUrl: string
}): Promise<void> {
  try {
    const createInput: CreateEnrollmentInput = {
      action: 'relogin',
      owner: input.active.owner
    }
    const enrollment = await input.client.createEnrollment(createInput)
    const enrollmentId = enrollment.enrollment_id
    const expiresAtMs = Date.parse(enrollment.expires_at)
    input.active.enrollmentId = enrollmentId
    input.active.expiresAtMs = expiresAtMs
    input.active.brokerStatus = enrollment.status
    if (input.activeEnrollments.get(input.actorKey) !== input.active) {
      await cancelEnrollmentQuietly(input.client, enrollmentId, input.options)
      return
    }
    if (input.active.state === 'cancel_requested') {
      input.active.state = 'cancelling'
      const cancelled = await input.client.cancelEnrollment(enrollmentId)
      if (cancelled?.status === 'importing') {
        input.active.brokerStatus = 'importing'
        input.active.state = 'monitoring'
        await postSlackResponse(
          input.fetchFn,
          input.responseUrl,
          'Codex login is already importing the canonical credential and can no longer be cancelled.',
          input.options.requestTimeoutMs
        )
        await monitorEnrollment(
          input.client,
          input.active,
          input.actorKey,
          input.activeEnrollments,
          input.options,
          input.fetchFn,
          input.pollIntervalMs
        )
        return
      }
      input.activeEnrollments.delete(input.actorKey)
      await postSlackResponse(
        input.fetchFn,
        input.responseUrl,
        cancelled && cancelled.status !== 'cancelled'
          ? formatTerminalEnrollment(cancelled)
          : 'Codex device login was cancelled.',
        input.options.requestTimeoutMs
      )
      return
    }
    input.active.state = 'monitoring'
    await postSlackResponse(
      input.fetchFn,
      input.responseUrl,
      enrollment.status === 'pending'
        ? formatDeviceCode(enrollment)
        : formatActiveEnrollment(enrollment),
      input.options.requestTimeoutMs
    )
    await monitorEnrollment(
      input.client,
      input.active,
      input.actorKey,
      input.activeEnrollments,
      input.options,
      input.fetchFn,
      input.pollIntervalMs
    )
  } catch (error) {
    if (input.activeEnrollments.get(input.actorKey) === input.active) {
      input.activeEnrollments.delete(input.actorKey)
    }
    if (input.active.enrollmentId) {
      await cancelEnrollmentQuietly(
        input.client,
        input.active.enrollmentId,
        input.options
      )
    }
    safeCommandWarning(input.options.logger, 'slackbotv2_autorotate_login_failed', error)
    try {
      await postSlackResponse(
        input.fetchFn,
        input.responseUrl,
        loginFailureText(error),
        input.options.requestTimeoutMs
      )
    } catch (deliveryError) {
      safeCommandWarning(
        input.options.logger,
        'slackbotv2_autorotate_login_error_delivery_failed',
        deliveryError
      )
    }
  }
}

async function monitorEnrollment(
  client: AutorotateClient,
  active: ActiveEnrollment,
  actorKey: string,
  activeEnrollments: Map<string, ActiveEnrollment>,
  options: AutorotateSlackOptions,
  fetchFn: SlackbotV2Fetch,
  pollIntervalMs: number
): Promise<void> {
  while (
    activeEnrollments.get(actorKey) === active
    && active.state === 'monitoring'
  ) {
    if (active.brokerStatus === 'pending' && Date.now() >= active.expiresAtMs) break
    await delay(pollIntervalMs)
    if (activeEnrollments.get(actorKey) !== active || active.state !== 'monitoring') return
    const enrollmentId = active.enrollmentId
    if (!enrollmentId) return
    let enrollment: EnrollmentStatusResponse
    try {
      enrollment = await client.enrollment(enrollmentId)
    } catch (error) {
      safeCommandWarning(options.logger, 'slackbotv2_autorotate_login_poll_failed', error)
      continue
    }
    if (activeEnrollments.get(actorKey) !== active || active.state !== 'monitoring') return
    if (enrollment.status === 'importing') {
      active.brokerStatus = 'importing'
    }
    if (!isTerminalStatus(enrollment.status)) continue

    activeEnrollments.delete(actorKey)
    await postSlackResponse(
      fetchFn,
      active.responseUrl,
      formatTerminalEnrollment(enrollment),
      options.requestTimeoutMs
    )
    return
  }

  if (activeEnrollments.get(actorKey) !== active || active.state !== 'monitoring') return
  activeEnrollments.delete(actorKey)
  await postSlackResponse(
    fetchFn,
    active.responseUrl,
    'Codex device login expired. Run `/autorotate login` to start again.',
    options.requestTimeoutMs
  )
}

async function respondWithEnrollmentStatus(input: {
  active?: ActiveEnrollment
  activeEnrollments: Map<string, ActiveEnrollment>
  actorKey: string
  client: AutorotateClient
  fetchFn: SlackbotV2Fetch
  options: AutorotateSlackOptions
  owner: string
  pollIntervalMs: number
  responseUrl: string
}): Promise<void> {
  try {
    const enrollment = input.active?.enrollmentId
      ? await input.client.enrollment(input.active.enrollmentId)
      : await input.client.activeEnrollment(input.owner)
    if (!enrollment) {
      await postSlackResponse(
        input.fetchFn,
        input.responseUrl,
        'You do not have an active Codex login.',
        input.options.requestTimeoutMs
      )
      return
    }
    if (isTerminalStatus(enrollment.status)) {
      if (!('account' in enrollment)) {
        throw new AutorotateError('Autorotate returned an invalid active enrollment')
      }
      if (input.activeEnrollments.get(input.actorKey) === input.active) {
        input.activeEnrollments.delete(input.actorKey)
      }
      await postSlackResponse(
        input.fetchFn,
        input.responseUrl,
        formatTerminalEnrollment(enrollment),
        input.options.requestTimeoutMs
      )
      return
    }
    if (!input.active && !('action' in enrollment)) {
      throw new AutorotateError('Autorotate returned an invalid active enrollment')
    }
    const active = input.active ?? activeEnrollmentFromBroker(
      enrollment as StartEnrollmentResponse,
      input.owner,
      input.responseUrl
    )
    if (!input.active) input.activeEnrollments.set(input.actorKey, active)
    if (enrollment.status !== 'pending' && enrollment.status !== 'importing') {
      throw new AutorotateError('Autorotate returned an invalid active enrollment')
    }
    active.responseUrl = input.responseUrl
    active.brokerStatus = enrollment.status
    active.state = 'monitoring'
    await postSlackResponse(
      input.fetchFn,
      input.responseUrl,
      formatActiveEnrollment(enrollment),
      input.options.requestTimeoutMs
    )
    await monitorEnrollment(
      input.client,
      active,
      input.actorKey,
      input.activeEnrollments,
      input.options,
      input.fetchFn,
      input.pollIntervalMs
    )
  } catch (error) {
    safeCommandWarning(
      input.options.logger,
      'slackbotv2_autorotate_login_status_failed',
      error
    )
  }
}

async function cancelEnrollment(input: {
  active?: ActiveEnrollment
  activeEnrollments: Map<string, ActiveEnrollment>
  actorKey: string
  client: AutorotateClient
  fetchFn: SlackbotV2Fetch
  options: AutorotateSlackOptions
  owner: string
  pollIntervalMs: number
  responseUrl: string
}): Promise<void> {
  try {
    const recovered = input.active?.enrollmentId
      ? null
      : await input.client.activeEnrollment(input.owner)
    const enrollmentId = input.active?.enrollmentId ?? recovered?.enrollment_id
    if (!enrollmentId) {
      if (input.activeEnrollments.get(input.actorKey) === input.active) {
        input.activeEnrollments.delete(input.actorKey)
      }
      await postSlackResponse(
        input.fetchFn,
        input.responseUrl,
        'You do not have an active Codex login.',
        input.options.requestTimeoutMs
      )
      return
    }
    const cancelled = await input.client.cancelEnrollment(enrollmentId)
    if (cancelled?.status === 'importing') {
      const active = input.active ?? (recovered
        ? activeEnrollmentFromBroker(recovered, input.owner, input.responseUrl)
        : null)
      if (!active) throw new AutorotateError('Autorotate returned an invalid active enrollment')
      active.brokerStatus = 'importing'
      active.responseUrl = input.responseUrl
      active.state = 'monitoring'
      input.activeEnrollments.set(input.actorKey, active)
      await postSlackResponse(
        input.fetchFn,
        input.responseUrl,
        'Codex login is already importing the canonical credential and can no longer be cancelled.',
        input.options.requestTimeoutMs
      )
      await monitorEnrollment(
        input.client,
        active,
        input.actorKey,
        input.activeEnrollments,
        input.options,
        input.fetchFn,
        input.pollIntervalMs
      )
      return
    }
    if (
      cancelled?.status
      && cancelled.status !== 'cancelled'
      && isTerminalStatus(cancelled.status)
    ) {
      if (input.activeEnrollments.get(input.actorKey) === input.active) {
        input.activeEnrollments.delete(input.actorKey)
      }
      await postSlackResponse(
        input.fetchFn,
        input.responseUrl,
        formatTerminalEnrollment(cancelled),
        input.options.requestTimeoutMs
      )
      return
    }
    if (cancelled?.status && cancelled.status !== 'cancelled') {
      throw new AutorotateError('Autorotate did not cancel enrollment')
    }
    if (input.activeEnrollments.get(input.actorKey) === input.active) {
      input.activeEnrollments.delete(input.actorKey)
    }
    await postSlackResponse(
      input.fetchFn,
      input.responseUrl,
      'Codex device login was cancelled.',
      input.options.requestTimeoutMs
    )
  } catch (error) {
    if (
      input.active
      && input.activeEnrollments.get(input.actorKey) === input.active
    ) {
      input.active.state = 'monitoring'
    }
    safeCommandWarning(
      input.options.logger,
      'slackbotv2_autorotate_login_cancel_failed',
      error
    )
    await postSlackResponse(
      input.fetchFn,
      input.responseUrl,
      'Codex device login could not be cancelled.',
      input.options.requestTimeoutMs
    )
  }
}

async function cancelEnrollmentQuietly(
  client: AutorotateClient,
  enrollmentId: string,
  options: AutorotateSlackOptions
): Promise<void> {
  try {
    await client.cancelEnrollment(enrollmentId)
  } catch (error) {
    safeCommandWarning(options.logger, 'slackbotv2_autorotate_login_cleanup_failed', error)
  }
}

function activeEnrollmentFromBroker(
  enrollment: StartEnrollmentResponse,
  owner: string,
  responseUrl: string
): ActiveEnrollment {
  const enrollmentId = enrollment.enrollment_id
  const expiresAtMs = Date.parse(enrollment.expires_at)
  return {
    brokerStatus: enrollment.status,
    enrollmentId,
    expiresAtMs,
    owner,
    responseUrl,
    state: 'monitoring'
  }
}

type ParsedCommand =
  | { kind: 'fleet' }
  | { kind: 'help' }
  | { kind: 'status' }
  | { kind: 'login' }
  | { kind: 'login_status' }
  | { kind: 'login_cancel' }
  | { kind: 'maintenance_status' }
  | { kind: 'maintenance_drain' }
  | { kind: 'maintenance_resume' }
  | { kind: 'trace_off' }
  | { kind: 'trace_on', durationMs: number }
  | { kind: 'trace_status' }

type TraceCommand = Extract<ParsedCommand, { kind: 'trace_off' | 'trace_on' | 'trace_status' }>

function isTraceCommand(command: ParsedCommand): command is TraceCommand {
  return command.kind === 'trace_off'
    || command.kind === 'trace_on'
    || command.kind === 'trace_status'
}

function isMaintenanceCommand(command: ParsedCommand): command is MaintenanceCommand {
  return command.kind === 'maintenance_status'
    || command.kind === 'maintenance_drain'
    || command.kind === 'maintenance_resume'
}

function isMaintenanceMutation(command: MaintenanceCommand): boolean {
  return command.kind === 'maintenance_drain' || command.kind === 'maintenance_resume'
}

function parseCommand(text: string): ParsedCommand {
  const parts = text.trim().split(/\s+/).filter(Boolean)
  const root = parts[0]?.toLowerCase()
  if (root === 'fleet' && parts.length === 1) return { kind: 'fleet' }
  if (root === 'maintenance') {
    const action = parts[1]?.toLowerCase()
    if (parts.length !== 2) return { kind: 'help' }
    if (action === 'status') return { kind: 'maintenance_status' }
    if (action === 'drain') return { kind: 'maintenance_drain' }
    if (action === 'resume') return { kind: 'maintenance_resume' }
    return { kind: 'help' }
  }
  if (root === 'trace') {
    const action = parts[1]?.toLowerCase()
    if (parts.length === 2 && action === 'status') return { kind: 'trace_status' }
    if (parts.length === 2 && action === 'off') return { kind: 'trace_off' }
    if (action === 'on' && (parts.length === 2 || parts.length === 3)) {
      const durationMs = parts.length === 2
        ? DEFAULT_TRACE_CONSENT_LIFETIME_MS
        : parseTraceConsentDuration(parts[2] ?? '')
      if (durationMs !== null) return { kind: 'trace_on', durationMs }
    }
    return { kind: 'help' }
  }
  if (parts.length === 1 && (root === 'status' || root === 'accounts')) {
    return { kind: 'status' }
  }
  if (parts.length === 1 && (root === 'login' || root === 'add' || root === 'relogin')) {
    return { kind: 'login' }
  }
  if (root !== 'login') return { kind: 'help' }
  if (parts.length === 2 && parts[1]?.toLowerCase() === 'status') return { kind: 'login_status' }
  if (parts.length === 2 && parts[1]?.toLowerCase() === 'cancel') return { kind: 'login_cancel' }
  return { kind: 'help' }
}

function helpText(): string {
  return [
    'Autorotate commands',
    '`/autorotate status` — account usability and rate limits',
    '`/autorotate login` — add or refresh the account you authenticate',
    '`/autorotate fleet` — redacted active consumers and terminal aggregates',
    '`/autorotate maintenance status` — current broker maintenance mode',
    '`/autorotate maintenance drain` — operators only; stop new broker work',
    '`/autorotate maintenance resume` — operators only; resume the current drain',
    '`/autorotate trace status` — show your trace setting',
    '`/autorotate trace on [duration]` — collect metadata for up to 24h (default: 1h)',
    '`/autorotate trace off` — stop trace collection'
  ].join('\n')
}

function parseTraceConsentDuration(value: string): number | null {
  const match = /^([1-9][0-9]*)([hm])$/i.exec(value)
  if (!match) return null
  const amount = Number(match[1])
  const multiplier = match[2]?.toLowerCase() === 'h' ? 60 * 60_000 : 60_000
  const durationMs = amount * multiplier
  return Number.isSafeInteger(durationMs)
    && durationMs <= MAX_TRACE_CONSENT_LIFETIME_MS
    ? durationMs
    : null
}

function traceConsentFailure(kind: TraceCommand['kind']): string {
  switch (kind) {
    case 'trace_status':
      return 'Your Slack trace setting is temporarily unavailable. Check `/autorotate trace status`.'
    case 'trace_on':
      return 'Your Slack trace setting could not be confirmed. Check `/autorotate trace status`.'
    case 'trace_off':
      return 'Trace collection revoke could not be confirmed. Check `/autorotate trace status`.'
  }
}

function formatTraceConsent(consent: SlackTraceConsent): string {
  if (consent.drain_pending) {
    return 'Slack trace collection was revoked, but the Kubernetes drain is pending. Check `/autorotate trace status`.'
  }
  if (!consent.enabled) {
    return 'Slack trace collection is off. When enabled, it collects only metadata, never message content.'
  }
  const expiresAt = consent.expires_at
  if (!expiresAt) throw new AutorotateError('Centaur API returned an invalid trace consent')
  return [
    `Slack trace collection is on until ${formatUtcTimestamp(expiresAt)}.`,
    'Only metadata is collected, never message content. It expires automatically.'
  ].join(' ')
}

function formatFleet(report: FleetReport): string {
  const sessions = [...report.active_sessions].sort((left, right) =>
    left.consumer_fingerprint.localeCompare(right.consumer_fingerprint)
  )
  const lines = [
    `Codex fleet: ${sessions.length} active consumer${sessions.length === 1 ? '' : 's'}`,
    ...sessions.flatMap((session, index) => [
      '',
      `Consumer ${index + 1}: ${session.consumer_fingerprint}`,
      `  client version: ${session.client_version ? escapeSlackText(session.client_version) : 'unknown'} — tell this consumer to bump if it is behind`,
      `  account: ${session.account_email ? escapeSlackText(session.account_email) : 'email unavailable'} (${escapeSlackText(session.account_label)})`,
      `  heartbeat: ${session.last_heartbeat_at ? formatUtcTimestamp(session.last_heartbeat_at) : 'not reported'}`,
      `  expires: ${formatUtcTimestamp(session.expires_at)}`
    ]),
    '',
    `Terminal leases: ${report.terminal_leases.total} total; clean ${report.terminal_leases.clean}, expired ${report.terminal_leases.expired}, expired unreleased ${report.terminal_leases.expired_unreleased}, unusable ${report.terminal_leases.unusable}, legacy ${report.terminal_leases.unknown_legacy}, other ${report.terminal_leases.other}`
  ]
  return truncateCommandText(lines.join('\n'))
}

function formatMaintenanceStatus(status: MaintenanceStatus): string {
  return [
    `Autorotate maintenance: ${status.mode} (control epoch ${status.control_epoch})`,
    `Changed: ${formatUtcTimestamp(status.changed_at)}`,
    `Active work: ${status.active_leases} leases, ${status.active_quota_probes} quota probes, ${status.active_enrollments} enrollments`,
    `Drain: ${status.quiescent ? 'quiescent' : 'waiting'}; ${formatDrainCompletion(status.drain_completion)}`,
    `Drain terminals: clean ${status.drain_clean_terminals}, expired ${status.drain_expired_terminals}, other ${status.drain_other_terminals}`
  ].join('\n')
}

function formatDrainCompletion(value: MaintenanceStatus['drain_completion']): string {
  switch (value) {
    case 'pending': return 'pending'
    case 'drained': return 'drained'
    case 'drained_with_unclean_sessions': return 'drained with unclean sessions'
    default: return 'not draining'
  }
}

function truncateCommandText(text: string): string {
  return text.length <= MAX_STATUS_TEXT_CHARS
    ? text
    : `${text.slice(0, MAX_STATUS_TEXT_CHARS - 36)}\n\nAdditional fleet rows not shown.`
}

function formatStatus(accounts: readonly AutorotateAccount[]): string {
  if (accounts.length === 0) return 'Codex accounts: 0 usable / 0\n\nNo accounts.'
  const sorted = [...accounts].sort(compareAccounts)
  const usable = accounts.filter(isUsableAccount).length
  const accountBlocks = sorted.map(account => {
    const email = account.email ? escapeSlackText(account.email) : 'email unknown'
    const state = isUsableAccount(account) ? 'usable' : 'unusable'
    return [
      `${email} [${escapeSlackText(account.label)}]`,
      `  state: ${state}`,
      `  reason: ${accountReason(account)}`,
      ...rateLimitRows(account),
      ...resetCreditRows(account.reset_credits),
      `  observed: ${limitsObservedAt(account)}`,
      `  next available: ${nextAvailable(account)}`
    ].join('\n')
  })
  let text = `Codex accounts: ${usable} usable / ${accounts.length}`
  let included = 0
  for (const block of accountBlocks) {
    const candidate = `${text}\n\n${block}`
    const remaining = accountBlocks.length - included - 1
    const suffix = remaining > 0 ? statusTruncationText(remaining) : ''
    if (candidate.length + suffix.length > MAX_STATUS_TEXT_CHARS) break
    text = candidate
    included += 1
  }
  const omitted = accountBlocks.length - included
  return omitted > 0 ? text + statusTruncationText(omitted) : text
}

function statusTruncationText(omitted: number): string {
  return `\n\n${omitted} more account${omitted === 1 ? '' : 's'} not shown.`
}

const ACCOUNT_AVAILABILITY_ORDER: Record<AutorotateAccount['availability'], number> = {
  login_required: 0,
  reconciliation_required: 1,
  rate_limited: 2,
  in_use: 3,
  available: 4,
  disabled: 5
}

function compareAccounts(left: AutorotateAccount, right: AutorotateAccount): number {
  const availability = ACCOUNT_AVAILABILITY_ORDER[left.availability]
    - ACCOUNT_AVAILABILITY_ORDER[right.availability]
  if (availability !== 0) return availability
  const leftIdentity = (left.email ?? left.label).toLowerCase()
  const rightIdentity = (right.email ?? right.label).toLowerCase()
  if (leftIdentity < rightIdentity) return -1
  if (leftIdentity > rightIdentity) return 1
  return left.label < right.label ? -1 : left.label > right.label ? 1 : 0
}

function accountReason(account: AutorotateAccount): string {
  if (account.unusable_reason) {
    switch (account.unusable_reason) {
      case 'refresh_token_expired':
        return 'refresh token expired'
      case 'refresh_token_reused':
        return 'refresh token reused'
      case 'refresh_token_revoked':
        return 'refresh token revoked'
      case 'account_mismatch':
        return 'account mismatch'
      case 'login_required':
        return 'login required'
      case 'reconciliation_required':
        return 'reconciliation required'
      case 'operator_reported':
        return 'reported unusable by operator'
    }
  }
  switch (account.availability) {
    case 'available':
      return 'available'
    case 'in_use':
      return 'available'
    case 'rate_limited':
      return 'out of rate limits'
    case 'login_required':
      return 'login required'
    case 'reconciliation_required':
      return 'reconciliation required'
    case 'disabled':
      return 'disabled by operator'
  }
}

function rateLimitRows(account: AutorotateAccount): string[] {
  const primary = {
    label: rateLimitLabel(account.primary, 'primary'),
    window: account.primary
  }
  const secondary = {
    label: rateLimitLabel(account.secondary, 'secondary'),
    window: account.secondary
  }

  if (primary.label === secondary.label) {
    if (!primary.window) {
      primary.label = otherCanonicalRateLimit(secondary.label)
    } else if (!secondary.window) {
      secondary.label = otherCanonicalRateLimit(primary.label)
    } else {
      secondary.label = 'secondary'
    }
  }

  return [primary, secondary]
    .sort((left, right) => rateLimitRank(left.label) - rateLimitRank(right.label))
    .map(({ label, window }) => `  ${label}: ${formatRateLimitWindow(window)}`)
}

function rateLimitLabel(window: RateLimitWindow | null, fallback: string): string {
  if (!window) return fallback === 'primary' ? '5h' : 'weekly'
  if (window.window_minutes === 300) return '5h'
  if (window.window_minutes === 10_080) return 'weekly'
  return fallback
}

function otherCanonicalRateLimit(label: string): string {
  return label === 'weekly' ? '5h' : 'weekly'
}

function rateLimitRank(label: string): number {
  if (label === '5h') return 0
  if (label === 'weekly') return 1
  if (label === 'primary') return 2
  return 3
}

function formatRateLimitWindow(window: RateLimitWindow | null): string {
  if (!window) return 'unknown'
  const resetsAt = window.resets_at ? formatUtcTimestamp(window.resets_at) : 'unknown'
  return `${formatPercent(window.used_percent)} used; resets ${resetsAt}`
}

function formatPercent(value: number): string {
  return `${Object.is(value, -0) ? '0' : String(value)}%`
}

function resetCreditRows(resetCredits: ResetCredits | null): string[] {
  if (!resetCredits) return ['  reset credits: unknown']
  const missingExpiryCount = resetCredits.available_count - resetCredits.credits.length
  return [
    `  reset credits: ${resetCredits.available_count} available`,
    ...resetCredits.credits.map((credit, index) => {
      const expiry = credit.expires_at
        ? `expires ${formatUtcTimestamp(credit.expires_at)}`
        : 'does not expire'
      return `    reset ${index + 1}: ${expiry}`
    }),
    ...(missingExpiryCount > 0
      ? [`    expiry unavailable for ${missingExpiryCount} ${missingExpiryCount === 1 ? 'credit' : 'credits'}`]
      : [])
  ]
}

function limitsObservedAt(account: AutorotateAccount): string {
  if (!account.primary && !account.secondary && !account.reset_credits) return 'unknown'
  return account.limits_observed_at
    ? formatUtcTimestamp(account.limits_observed_at)
    : 'unknown'
}

function nextAvailable(account: AutorotateAccount): string {
  if (isUsableAccount(account)) return 'now'
  if (account.availability === 'login_required') return 'run /autorotate login'
  if (account.availability === 'reconciliation_required') return 'operator reconciliation required'
  if (account.availability === 'disabled') return 'operator action required'
  return account.next_available_at
    ? formatUtcTimestamp(account.next_available_at)
    : 'unknown'
}

function isUsableAccount(account: AutorotateAccount): boolean {
  return account.availability === 'available' || account.availability === 'in_use'
}

function formatUtcTimestamp(value: string): string {
  return `${new Date(value).toISOString().slice(0, 16).replace('T', ' ')} UTC`
}

function formatDeviceCode(enrollment: StartEnrollmentResponse): string {
  const verificationUrl = safeVerificationUrl(enrollment.verification_url)
  const userCode = safeUserCode(enrollment.user_code)
  if (!verificationUrl || !userCode) throw new AutorotateError('invalid device authorization')
  return [
    '*Codex device login*',
    `Open <${verificationUrl}|the OpenAI device login page> and enter: \`${userCode}\``,
    safeTimestamp(enrollment.expires_at) ? `Expires: ${safeTimestamp(enrollment.expires_at)}` : null,
    'This ephemeral response will be replaced when login completes.'
  ].filter(Boolean).join('\n')
}

function formatActiveEnrollment(
  enrollment: StartEnrollmentResponse | EnrollmentStatusResponse
): string {
  if (enrollment.status === 'importing') {
    const label = 'account_label' in enrollment && typeof enrollment.account_label === 'string'
      ? ` for \`${escapeSlackText(enrollment.account_label)}\``
      : ''
    return `Codex login${label} is authorized and importing the canonical credential.`
  }
  if ('verification_url' in enrollment && 'user_code' in enrollment) {
    return formatDeviceCode(enrollment)
  }
  return 'Codex login is still waiting for device authorization.'
}

function loginFailureText(error: unknown): string {
  const code = error instanceof AutorotateError ? error.code : undefined
  return enrollmentFailureText(
    code,
    'Codex device login could not be started. Try again or ask an Autorotate operator.'
  )
}

function enrollmentFailureText(errorCode: string | null | undefined, fallback: string): string {
  switch (errorCode) {
    case 'account_not_found':
      return 'Autorotate could not add or refresh that Codex account. Run `/autorotate login` to try again.'
    case 'account_busy':
      return 'That Codex account is already being reauthenticated. Try again after the active login finishes.'
    case 'account_mismatch':
      return 'Autorotate could not safely match that Codex login. Check `/autorotate status` and try again.'
    case 'enrollment_already_active':
      return 'Another Codex login is already active. Wait for it to finish, or try again after it expires.'
    case 'email_mismatch':
      return 'The expected email does not match that Codex account.'
    default:
      return fallback
  }
}

function formatTerminalEnrollment(enrollment: EnrollmentStatusResponse): string {
  switch (enrollment.status) {
    case 'completed':
      return completedEnrollmentText(enrollment)
    case 'cancelled':
      return 'Codex device login was cancelled.'
    case 'expired':
      return 'Codex device login expired. Run `/autorotate login` to start again.'
    case 'failed':
      return enrollmentFailureText(
        enrollment.error_code,
        'Codex device login failed. Run `/autorotate login` to try again.'
      )
    default:
      return 'Codex device login failed.'
  }
}

function completedEnrollmentText(enrollment: EnrollmentStatusResponse): string {
  if (!enrollment.account) throw new AutorotateError('Autorotate returned invalid completion')
  const label = `\`${escapeSlackText(enrollment.account.label)}\``
  const email = safeEmail(enrollment.account.email)
    ? ` (${escapeSlackText(enrollment.account.email)})`
    : ''
  return `Codex account ${label}${email} is ready in Autorotate.`
}

function validateStartEnrollment(payload: JsonObject): StartEnrollmentResponse {
  const enrollmentId = payload.enrollment_id
  const action = payload.action
  const accountLabel = payload.account_label
  const expiresAt = payload.expires_at
  const status = payload.status
  const verificationUrl = payload.verification_url
  const userCode = payload.user_code
  if (
    typeof enrollmentId !== 'string'
    || !ENROLLMENT_ID_PATTERN.test(enrollmentId)
    || (action !== 'add' && action !== 'relogin')
    || (accountLabel !== undefined && !safeAccountLabel(accountLabel))
    || typeof expiresAt !== 'string'
    || !safeTimestamp(expiresAt)
    || (status !== 'pending' && status !== 'importing')
  ) {
    throw new AutorotateError('Autorotate returned an invalid enrollment')
  }
  if (status === 'pending') {
    if (!safeVerificationUrl(verificationUrl) || !safeUserCode(userCode)) {
      throw new AutorotateError('invalid device authorization')
    }
  } else if (verificationUrl !== undefined || userCode !== undefined) {
    throw new AutorotateError('Autorotate returned device authorization after pending')
  }
  return {
    enrollment_id: enrollmentId,
    action,
    ...(typeof accountLabel === 'string' ? { account_label: accountLabel } : {}),
    ...(typeof verificationUrl === 'string' ? { verification_url: verificationUrl } : {}),
    ...(typeof userCode === 'string' ? { user_code: userCode } : {}),
    expires_at: expiresAt,
    status
  }
}

function validateEnrollmentStatus(payload: JsonObject): EnrollmentStatusResponse {
  const enrollmentId = payload.enrollment_id
  const status = payload.status
  const expiresAt = payload.expires_at
  const account = validateEnrollmentAccount(payload.account)
  const errorCode = payload.error_code
  if (
    typeof enrollmentId !== 'string'
    || !ENROLLMENT_ID_PATTERN.test(enrollmentId)
    || !isEnrollmentStatus(status)
    || typeof expiresAt !== 'string'
    || !safeTimestamp(expiresAt)
    || !Object.hasOwn(payload, 'account')
    || !Object.hasOwn(payload, 'error_code')
    || (errorCode !== null && !safeErrorCode(errorCode))
    || (status === 'completed') !== (account !== null)
  ) {
    throw new AutorotateError('Autorotate returned an invalid enrollment status')
  }
  return {
    enrollment_id: enrollmentId,
    status,
    expires_at: expiresAt,
    account,
    error_code: errorCode
  }
}

function validateEnrollmentAccount(value: unknown): EnrollmentAccount | null {
  if (value === null) return null
  if (!isJsonObject(value)) {
    throw new AutorotateError('Autorotate returned invalid account identity')
  }
  const safeAccount = selectFields(value, ENROLLMENT_ACCOUNT_FIELDS)
  const label = safeAccount.label
  const email = safeAccount.email
  const status = safeAccount.status
  if (!safeAccountLabel(label) || (email !== null && !safeEmail(email))) {
    throw new AutorotateError('Autorotate returned invalid account identity')
  }
  if (!isAccountStatus(status)) {
    throw new AutorotateError('Autorotate returned invalid account identity')
  }
  return { label, email, status }
}

function validateAccount(payload: JsonObject): AutorotateAccount {
  const label = payload.label
  const email = payload.email
  const status = payload.status
  const limitedUntil = payload.limited_until
  const loginRequired = payload.login_required
  const reconciliationRequired = payload.reconciliation_required
  const activeWriter = payload.active_writer
  const availability = payload.availability
  const unusableReason = payload.unusable_reason
  const limitsObservedAt = payload.limits_observed_at
  const primary = validateRateLimitWindow(payload.primary)
  const resetCredits = validateResetCredits(payload.reset_credits)
  const secondary = validateRateLimitWindow(payload.secondary)
  const nextAvailableAt = payload.next_available_at
  if (
    !REQUIRED_ACCOUNT_FIELDS.every(field => Object.hasOwn(payload, field))
    || !safeAccountLabel(label)
    || (email !== null && !safeEmail(email))
    || !isAccountStatus(status)
    || (limitedUntil !== null && (typeof limitedUntil !== 'string' || !safeTimestamp(limitedUntil)))
    || typeof loginRequired !== 'boolean'
    || typeof reconciliationRequired !== 'boolean'
    || typeof activeWriter !== 'boolean'
    || !isAccountAvailability(availability)
    || (unusableReason !== null && !isAccountUnusableReason(unusableReason))
    || (
      limitsObservedAt !== null
      && (typeof limitsObservedAt !== 'string' || !safeTimestamp(limitsObservedAt))
    )
    || (
      nextAvailableAt !== null
      && (typeof nextAvailableAt !== 'string' || !safeTimestamp(nextAvailableAt))
    )
  ) {
    throw new AutorotateError('Autorotate returned invalid accounts')
  }
  const account: AutorotateAccount = {
    active_writer: activeWriter,
    availability,
    label,
    email,
    status,
    limited_until: limitedUntil,
    limits_observed_at: limitsObservedAt,
    login_required: loginRequired,
    next_available_at: nextAvailableAt,
    primary,
    reconciliation_required: reconciliationRequired,
    reset_credits: resetCredits,
    secondary,
    unusable_reason: unusableReason
  }
  if (!validAccountState(account)) {
    throw new AutorotateError('Autorotate returned contradictory account status')
  }
  return account
}

function validAccountState(account: AutorotateAccount): boolean {
  if (
    (account.primary || account.secondary || account.reset_credits)
    && !account.limits_observed_at
  ) return false
  switch (account.availability) {
    case 'available':
      return account.status === 'enabled'
        && !account.login_required
        && !account.reconciliation_required
        && !account.active_writer
        && account.limited_until === null
        && account.unusable_reason === null
        && account.next_available_at === null
    case 'in_use':
      return account.status === 'enabled'
        && !account.login_required
        && !account.reconciliation_required
        && account.active_writer
        && account.limited_until === null
        && account.unusable_reason === null
        && account.next_available_at !== null
    case 'rate_limited':
      return account.status === 'enabled'
        && !account.login_required
        && !account.reconciliation_required
        && account.limited_until !== null
        && account.unusable_reason === null
        && account.next_available_at !== null
        && Date.parse(account.next_available_at) >= Date.parse(account.limited_until)
    case 'login_required':
      return (account.status === 'dead' || account.status === 'disabled')
        && account.login_required
        && !account.reconciliation_required
        && account.unusable_reason !== null
        && account.next_available_at === null
    case 'reconciliation_required':
      return account.status === 'disabled'
        && !account.login_required
        && account.reconciliation_required
        && account.unusable_reason === 'reconciliation_required'
        && account.next_available_at === null
    case 'disabled':
      return account.status === 'disabled'
        && !account.login_required
        && !account.reconciliation_required
        && account.unusable_reason === null
        && account.next_available_at === null
  }
}

function validateRateLimitWindow(value: unknown): RateLimitWindow | null {
  if (value === null) return null
  if (!isJsonObject(value)) {
    throw new AutorotateError('Autorotate returned invalid accounts')
  }
  const window = selectFields(value, RATE_LIMIT_FIELDS)
  const usedPercent = window.used_percent
  const resetsAt = window.resets_at
  const windowMinutes = window.window_minutes
  if (
    !RATE_LIMIT_FIELDS.every(field => Object.hasOwn(window, field))
    || typeof usedPercent !== 'number'
    || !Number.isFinite(usedPercent)
    || usedPercent < 0
    || usedPercent > 100
    || (resetsAt !== null && (typeof resetsAt !== 'string' || !safeTimestamp(resetsAt)))
    || (
      windowMinutes !== null
      && (!Number.isSafeInteger(windowMinutes) || (windowMinutes as number) <= 0)
    )
  ) {
    throw new AutorotateError('Autorotate returned invalid accounts')
  }
  return {
    resets_at: resetsAt,
    used_percent: usedPercent,
    window_minutes: windowMinutes as number | null
  }
}

function validateResetCredits(value: unknown): ResetCredits | null {
  if (value === undefined || value === null) return null
  if (!isJsonObject(value)) {
    throw new AutorotateError('Autorotate returned invalid reset credits')
  }
  const payload = selectFields(value, RESET_CREDITS_FIELDS)
  const availableCount = payload.available_count
  const credits = payload.credits
  if (
    typeof availableCount !== 'number'
    || !Number.isSafeInteger(availableCount)
    || availableCount < 0
    || !Array.isArray(credits)
    || credits.length > availableCount
  ) {
    throw new AutorotateError('Autorotate returned invalid reset credits')
  }
  return {
    available_count: availableCount,
    credits: credits.map(validateResetCredit)
  }
}

function validateResetCredit(value: unknown): ResetCredit {
  if (!isJsonObject(value)) {
    throw new AutorotateError('Autorotate returned invalid reset credit')
  }
  const payload = selectFields(value, RESET_CREDIT_FIELDS)
  const expiresAt = payload.expires_at
  if (
    !Object.hasOwn(payload, 'expires_at')
    || (expiresAt !== null && !safeTimestamp(expiresAt))
  ) {
    throw new AutorotateError('Autorotate returned invalid reset credit')
  }
  return {
    expires_at: expiresAt as string | null
  }
}

function selectFields(
  payload: JsonObject,
  fields: readonly string[]
): JsonObject {
  return Object.fromEntries(
    fields
      .filter(field => payload[field] !== undefined)
      .map(field => [field, payload[field]])
  ) as JsonObject
}

function parseJsonObject(text: string): JsonObject {
  try {
    const parsed: unknown = JSON.parse(text)
    if (!isJsonObject(parsed)) throw new Error('not an object')
    return parsed
  } catch {
    throw new AutorotateError('Autorotate returned invalid JSON')
  }
}

function tryParseJsonObject(text: string): JsonObject | null {
  try {
    const parsed: unknown = JSON.parse(text)
    return isJsonObject(parsed) ? parsed : null
  } catch {
    return null
  }
}

function parseBrokerUrl(value: string): URL {
  let url: URL
  try {
    url = new URL(value)
  } catch {
    throw new AutorotateError('Autorotate URL is invalid')
  }
  if (url.protocol !== 'https:' && !['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)) {
    throw new AutorotateError('Autorotate URL must use HTTPS')
  }
  if (url.username || url.password || url.search || url.hash) {
    throw new AutorotateError('Autorotate URL is invalid')
  }
  if (!url.pathname.endsWith('/')) url.pathname += '/'
  return url
}

function parseApiUrl(value: string): URL {
  let url: URL
  try {
    url = new URL(value)
  } catch {
    throw new AutorotateError('Centaur API URL is invalid')
  }
  if ((url.protocol !== 'http:' && url.protocol !== 'https:') || url.username || url.password) {
    throw new AutorotateError('Centaur API URL is invalid')
  }
  url.search = ''
  url.hash = ''
  return url
}

function validSlackSignature(
  headers: Headers,
  body: string,
  signingSecret: string,
  nowSeconds = Math.floor(Date.now() / 1000)
): boolean {
  const timestamp = headers.get('x-slack-request-timestamp') ?? ''
  const signature = headers.get('x-slack-signature') ?? ''
  const parsedTimestamp = verifiedSlackRequestTimestamp(headers)
  if (parsedTimestamp === null) return false
  if (Math.abs(nowSeconds - parsedTimestamp) > MAX_SLACK_REQUEST_AGE_SECONDS) return false
  if (!/^v0=[0-9a-f]{64}$/.test(signature)) return false

  const expected = `v0=${createHmac('sha256', signingSecret)
    .update(`v0:${timestamp}:${body}`)
    .digest('hex')}`
  const actualBuffer = Buffer.from(signature)
  const expectedBuffer = Buffer.from(expected)
  return actualBuffer.length === expectedBuffer.length
    && timingSafeEqual(actualBuffer, expectedBuffer)
}

function verifiedSlackRequestTimestamp(headers: Headers): number | null {
  const timestamp = headers.get('x-slack-request-timestamp') ?? ''
  if (!/^\d+$/.test(timestamp)) return null
  const parsedTimestamp = Number(timestamp)
  return Number.isSafeInteger(parsedTimestamp) ? parsedTimestamp : null
}

function authorizedWorkspace(
  options: AutorotateSlackOptions,
  teamId: string,
  userId: string
): boolean {
  if (!SLACK_TEAM_ID_PATTERN.test(teamId) || !SLACK_MEMBER_ID_PATTERN.test(userId)) return false
  const teams = new Set(options.operatorSlackTeamIds ?? [])
  return teams.has(teamId)
}

function authorizedMaintenanceOperator(
  options: AutorotateSlackOptions,
  teamId: string,
  userId: string
): boolean {
  if (!SLACK_TEAM_ID_PATTERN.test(teamId) || !SLACK_MEMBER_ID_PATTERN.test(userId)) return false
  return new Set(options.operatorSlackUserIds ?? []).has(`${teamId}:${userId}`)
}

function safeResponseUrl(
  value: string | null,
  options: AutorotateSlackOptions
): string | null {
  if (!value) return null
  try {
    const url = new URL(value)
    const hosts = new Set(options.responseUrlHosts ?? ['hooks.slack.com'])
    if (url.protocol !== 'https:' || !hosts.has(url.hostname) || url.username || url.password) {
      return null
    }
    return url.toString()
  } catch {
    return null
  }
}

function traceOffIdempotencyKey(headers: Headers, signingSecret: string): string {
  const signature = headers.get('x-slack-signature') ?? ''
  return `slack-trace-off:${createHmac('sha256', signingSecret).update(signature).digest('hex')}`
}

function maintenanceRequestId(
  headers: Headers,
  signingSecret: string,
  action: MaintenanceCommand['kind']
): string {
  const signature = headers.get('x-slack-signature') ?? ''
  const bytes = createHmac('sha256', signingSecret)
    .update(`maintenance:${action}:${signature}`)
    .digest('hex')
  return `${bytes.slice(0, 8)}-${bytes.slice(8, 12)}-4${bytes.slice(13, 16)}-${((Number.parseInt(bytes[16] ?? '0', 16) & 0x3) | 0x8).toString(16)}${bytes.slice(17, 20)}-${bytes.slice(20, 32)}`
}

function maintenanceAcknowledgement(command: MaintenanceCommand['kind']): string {
  switch (command) {
    case 'maintenance_status': return 'Checking Autorotate maintenance status…'
    case 'maintenance_drain': return 'Requesting Autorotate maintenance drain…'
    case 'maintenance_resume': return 'Requesting Autorotate maintenance resume…'
  }
}

function safeVerificationUrl(value: unknown): string | null {
  if (typeof value !== 'string') return null
  try {
    const url = new URL(value)
    if (
      url.protocol !== 'https:'
      || !OPENAI_DEVICE_LOGIN_HOSTS.has(url.hostname)
      || url.username
      || url.password
    ) {
      return null
    }
    return url.toString()
  } catch {
    return null
  }
}

function safeUserCode(value: unknown): string | null {
  return typeof value === 'string' && /^[A-Z0-9-]{4,32}$/.test(value) ? value : null
}

function safeEmail(value: unknown): value is string {
  return typeof value === 'string'
    && value.length <= 254
    && /^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(value)
}

function safeAccountLabel(value: unknown): value is string {
  return typeof value === 'string'
    && value.length >= 1
    && value.length <= 128
    && !value.includes('\0')
}

function safeErrorCode(value: unknown): value is string {
  return typeof value === 'string'
    && /^[a-z0-9_]{1,128}$/.test(value)
}

function isAccountStatus(value: unknown): value is AutorotateAccount['status'] {
  return value === 'enabled' || value === 'disabled' || value === 'dead'
}

function isAccountAvailability(
  value: unknown
): value is AutorotateAccount['availability'] {
  return value === 'available'
    || value === 'in_use'
    || value === 'rate_limited'
    || value === 'login_required'
    || value === 'reconciliation_required'
    || value === 'disabled'
}

function isAccountUnusableReason(value: unknown): value is AccountUnusableReason {
  return value === 'refresh_token_expired'
    || value === 'refresh_token_reused'
    || value === 'refresh_token_revoked'
    || value === 'account_mismatch'
    || value === 'login_required'
    || value === 'reconciliation_required'
    || value === 'operator_reported'
}

function isEnrollmentStatus(value: unknown): value is EnrollmentStatus {
  return value === 'pending'
    || value === 'importing'
    || value === 'completed'
    || value === 'failed'
    || value === 'cancelled'
    || value === 'expired'
}

function safeTimestamp(value: unknown): string | null {
  if (typeof value !== 'string') return null
  const milliseconds = Date.parse(value)
  return Number.isFinite(milliseconds) ? new Date(milliseconds).toISOString() : null
}

function isTerminalStatus(value: EnrollmentStatus | undefined): boolean {
  return ['completed', 'expired', 'failed', 'cancelled'].includes(value ?? '')
}

function validateTraceConsent(
  payload: JsonObject,
  expectedWorkspaceId: string,
  expectedUserId: string
): SlackTraceConsent {
  const data = payload.data
  if (!isJsonObject(data)) {
    throw new AutorotateError('Centaur API returned an invalid trace consent')
  }
  const workspaceId = data.workspace_id
  const userId = data.user_id
  const enabled = data.enabled
  const expiresAt = data.expires_at
  const revision = data.revision
  const drainPending = data.drain_pending
  const parsedExpiry = expiresAt === null ? null : safeTimestamp(expiresAt)
  const now = Date.now()
  const expiresAtMs = parsedExpiry === null ? null : Date.parse(parsedExpiry)
  if (
    workspaceId !== expectedWorkspaceId
    || userId !== expectedUserId
    || typeof enabled !== 'boolean'
    || typeof revision !== 'number'
    || !Number.isSafeInteger(revision)
    || revision < 0
    || (drainPending !== undefined && typeof drainPending !== 'boolean')
    || (expiresAt !== null && parsedExpiry === null)
    || (enabled && (expiresAtMs === null || expiresAtMs <= now || expiresAtMs > now + MAX_TRACE_CONSENT_LIFETIME_MS || revision <= 0))
    || (!enabled && expiresAtMs !== null && expiresAtMs > now)
    || (drainPending === true && enabled)
  ) {
    throw new AutorotateError('Centaur API returned an invalid trace consent')
  }
  return {
    drain_pending: drainPending === true,
    enabled,
    expires_at: parsedExpiry,
    revision,
    user_id: userId,
    workspace_id: workspaceId
  }
}

function validateFleetReport(payload: JsonObject): FleetReport {
  const generatedAt = safeTimestamp(payload.generated_at)
  const sessions = payload.active_sessions
  const terminals = payload.terminal_leases
  if (!generatedAt || !Array.isArray(sessions) || !isJsonObject(terminals)) {
    throw new AutorotateError('Autorotate returned an invalid fleet report')
  }
  if (sessions.length > 500) throw new AutorotateError('Autorotate fleet report is too large')
  const terminalLeases = validateTerminalLeaseCounts(terminals)
  return {
    active_sessions: sessions.map(validateFleetSession),
    generated_at: generatedAt,
    terminal_leases: terminalLeases
  }
}

function validateFleetSession(value: unknown): FleetSession {
  if (!isJsonObject(value)) throw new AutorotateError('Autorotate returned an invalid fleet session')
  const accountEmail = value.account_email
  const accountLabel = value.account_label
  const clientVersion = value.client_version
  const fingerprint = value.consumer_fingerprint
  const createdAt = safeTimestamp(value.created_at)
  const expiresAt = safeTimestamp(value.expires_at)
  const heartbeatAt = value.last_heartbeat_at === null ? null : safeTimestamp(value.last_heartbeat_at)
  if (
    !safeAccountLabel(accountLabel)
    || (accountEmail !== null && !safeEmail(accountEmail))
    || (clientVersion !== null && !safeClientVersion(clientVersion))
    || !safeConsumerFingerprint(fingerprint)
    || !createdAt
    || !expiresAt
    || (value.last_heartbeat_at !== null && !heartbeatAt)
    || Date.parse(expiresAt) <= Date.parse(createdAt)
  ) throw new AutorotateError('Autorotate returned an invalid fleet session')
  return {
    account_email: accountEmail,
    account_label: accountLabel,
    client_version: clientVersion,
    consumer_fingerprint: fingerprint,
    created_at: createdAt,
    expires_at: expiresAt,
    last_heartbeat_at: heartbeatAt
  }
}

function validateTerminalLeaseCounts(payload: JsonObject): TerminalLeaseCounts {
  const fields = ['clean', 'expired', 'expired_unreleased', 'other', 'total', 'unknown_legacy', 'unusable'] as const
  if (!fields.every(field => safeCount(payload[field]))) {
    throw new AutorotateError('Autorotate returned invalid fleet terminal aggregates')
  }
  const counts = Object.fromEntries(fields.map(field => [field, payload[field]])) as TerminalLeaseCounts
  if (counts.clean + counts.expired + counts.expired_unreleased + counts.other + counts.unknown_legacy + counts.unusable !== counts.total) {
    throw new AutorotateError('Autorotate returned contradictory fleet terminal aggregates')
  }
  return counts
}

function validateMaintenanceStatus(payload: JsonObject): MaintenanceStatus {
  const mode = payload.mode
  const controlEpoch = payload.control_epoch ?? payload.epoch
  const changedAt = safeTimestamp(payload.changed_at)
  const completion = payload.drain_completion === undefined ? null : payload.drain_completion
  const reasonCode = payload.reason_code === undefined ? null : payload.reason_code
  if (
    (mode !== 'serving' && mode !== 'draining')
    || !safeEpoch(controlEpoch)
    || !changedAt
    || !safeCount(payload.active_leases)
    || !safeCount(payload.active_quota_probes)
    || !safeCount(payload.active_enrollments)
    || !safeCount(payload.drain_clean_terminals)
    || !safeCount(payload.drain_expired_terminals)
    || !safeCount(payload.drain_other_terminals)
    || typeof payload.quiescent !== 'boolean'
    || (completion !== null && completion !== 'pending' && completion !== 'drained' && completion !== 'drained_with_unclean_sessions')
    || (reasonCode !== null && !safeErrorCode(reasonCode))
    || (mode === 'serving' && completion !== null)
  ) throw new AutorotateError('Autorotate returned invalid maintenance status')
  return {
    active_enrollments: payload.active_enrollments,
    active_leases: payload.active_leases,
    active_quota_probes: payload.active_quota_probes,
    changed_at: changedAt,
    control_epoch: controlEpoch,
    drain_clean_terminals: payload.drain_clean_terminals,
    drain_completion: completion,
    drain_expired_terminals: payload.drain_expired_terminals,
    drain_other_terminals: payload.drain_other_terminals,
    mode,
    quiescent: payload.quiescent,
    reason_code: reasonCode
  }
}

function validateMaintenanceOperationResult(
  payload: JsonObject,
  expectedOperation: MaintenanceOperation
): MaintenanceStatus {
  const operation = payload.operation
  const status = payload.status
  if (operation !== expectedOperation || !isJsonObject(status)) {
    throw new AutorotateError('Autorotate returned an invalid maintenance request result')
  }
  return validateMaintenanceStatus(status)
}

function safeCount(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0
}

function safeEpoch(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0
}

function safeConsumerFingerprint(value: unknown): value is string {
  return typeof value === 'string' && /^sha256:[a-f0-9]{64}$/.test(value)
}

function safeClientVersion(value: unknown): value is string {
  return typeof value === 'string' && value.length >= 1 && value.length <= 128 && /^[A-Za-z0-9._+-]+$/.test(value)
}

function retryableMaintenanceMutationError(error: unknown): boolean {
  if (!(error instanceof AutorotateError)) return false
  return error.status === undefined || [408, 429, 502, 503, 504].includes(error.status)
}

function maintenanceRequestHashConflict(error: unknown): boolean {
  return error instanceof AutorotateError
    && error.status === 409
    && error.code === 'maintenance_request_hash_mismatch'
}

function jitteredMaintenanceDelay(attempt: number, deadline: number): number {
  const maximum = Math.max(0, deadline - Date.now())
  const delayMs = MAINTENANCE_RETRY_BASE_DELAY_MS * (attempt + 1) + Math.floor(Math.random() * MAINTENANCE_RETRY_BASE_DELAY_MS)
  return Math.min(delayMs, maximum)
}

function escapeSlackText(value: string): string {
  return value
    .replace(/[\u0000-\u001f\u007f]/g, character =>
      `\\u${character.charCodeAt(0).toString(16).padStart(4, '0')}`)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
}

function ephemeralResponse(text: string): Response {
  return Response.json(
    { response_type: 'ephemeral', text },
    { headers: { 'cache-control': 'no-store' } }
  )
}

async function postSlackResponse(
  fetchFn: SlackbotV2Fetch,
  responseUrl: string,
  text: string,
  timeoutMs = DEFAULT_REQUEST_TIMEOUT_MS
): Promise<void> {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), timeoutMs)
  try {
    const response = await fetchFn(responseUrl, {
      method: 'POST',
      headers: {
        'cache-control': 'no-store',
        'content-type': 'application/json'
      },
      body: JSON.stringify({
        delete_original: false,
        replace_original: true,
        response_type: 'ephemeral',
        text
      }),
      redirect: 'error',
      signal: controller.signal
    })
    if (!response.ok) throw new AutorotateError(`Slack response returned HTTP ${response.status}`)
  } finally {
    clearTimeout(timeout)
  }
}

function safeCommandWarning(logger: Logger, event: string, error: unknown): void {
  logger.warn(event, {
    error_code: error instanceof AutorotateError ? error.code : undefined,
    upstream_status: error instanceof AutorotateError ? error.status : undefined
  })
}

function delay(milliseconds: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, milliseconds))
}
