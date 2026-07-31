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
  brokerUrl?: string
  fetch?: SlackbotV2Fetch
  logger: Logger
  operatorSlackTeamIds?: readonly string[]
  operatorToken?: string
  pollIntervalMs?: number
  requestTimeoutMs?: number
  responseUrlHosts?: readonly string[]
  signingSecret: string
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

  return {
    async handle(request, waitUntil) {
      const rawBody = await request.clone().text()
      const form = new URLSearchParams(rawBody)
      if (form.get('command') !== AUTOROTATE_COMMAND) return null

      if (!validSlackSignature(request.headers, rawBody, options.signingSecret)) {
        return new Response('invalid Slack signature', { status: 401 })
      }

      const teamId = form.get('team_id')?.trim() ?? ''
      const userId = form.get('user_id')?.trim() ?? ''
      if (!authorizedWorkspace(options, teamId, userId)) {
        return ephemeralResponse('You are not authorized to operate the Codex account pool.')
      }
      if (!client) {
        return ephemeralResponse('Autorotate is not configured for this deployment.')
      }

      const command = parseCommand(form.get('text') ?? '')
      const actorKey = `${teamId}:${userId}`
      const owner = `slack:${teamId}:${userId}`
      if (command.kind === 'help') return ephemeralResponse(helpText())
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
  | { kind: 'help' }
  | { kind: 'status' }
  | { kind: 'login' }
  | { kind: 'login_status' }
  | { kind: 'login_cancel' }

function parseCommand(text: string): ParsedCommand {
  const parts = text.trim().split(/\s+/).filter(Boolean)
  const root = parts[0]?.toLowerCase()
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
    '`/autorotate login` — add or refresh the account you authenticate'
  ].join('\n')
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

function validSlackSignature(
  headers: Headers,
  body: string,
  signingSecret: string,
  nowSeconds = Math.floor(Date.now() / 1000)
): boolean {
  const timestamp = headers.get('x-slack-request-timestamp') ?? ''
  const signature = headers.get('x-slack-signature') ?? ''
  const parsedTimestamp = Number.parseInt(timestamp, 10)
  if (!Number.isSafeInteger(parsedTimestamp)) return false
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

function authorizedWorkspace(
  options: AutorotateSlackOptions,
  teamId: string,
  userId: string
): boolean {
  if (!SLACK_TEAM_ID_PATTERN.test(teamId) || !SLACK_MEMBER_ID_PATTERN.test(userId)) return false
  const teams = new Set(options.operatorSlackTeamIds ?? [])
  return teams.has(teamId)
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

function escapeSlackText(value: string): string {
  return value
    .replace(/[\u0000-\u001f\u007f]/g, character =>
      `\\u${character.charCodeAt(0).toString(16).padStart(4, '0')}`)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
}

function ephemeralResponse(text: string): Response {
  return Response.json({ response_type: 'ephemeral', text })
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
      headers: { 'content-type': 'application/json' },
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
