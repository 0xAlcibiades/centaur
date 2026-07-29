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
const STATUS_FIELDS = [
  'generated_at',
  'total',
  'healthy',
  'available',
  'limited',
  'login_required',
  'disabled',
  'leased',
  'removed',
  'next_available_at',
  'pending_enrollments'
] as const
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
  'login_required'
] as const
const ENROLLMENT_ID_PATTERN = /^[A-Za-z0-9_-]{8,128}$/
const SLACK_MEMBER_ID_PATTERN = /^[UW][A-Z0-9]+$/i
const SLACK_TEAM_ID_PATTERN = /^T[A-Z0-9]+$/i
const OPENAI_DEVICE_LOGIN_HOSTS = new Set(['auth.openai.com'])

type AutorotateStatus = {
  generated_at?: string
  total?: number
  healthy?: number
  available?: number
  limited?: number
  login_required?: number
  disabled?: number
  leased?: number
  removed?: number
  next_available_at?: string | null
  pending_enrollments?: number
}

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
  label: string
  email: string | null
  status: 'enabled' | 'disabled' | 'dead'
  limited_until: string | null
  login_required: boolean
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

type CreateEnrollmentInput =
  | { action: 'add'; expected_email?: string; label?: string; owner: string }
  | { action: 'relogin'; account?: string; expected_email?: string; owner: string }

type AutorotateSlackOptions = {
  brokerUrl?: string
  fetch?: SlackbotV2Fetch
  logger: Logger
  observerToken?: string
  operatorSlackTeamIds?: readonly string[]
  operatorSlackUserIds?: readonly string[]
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
    private readonly observerToken: string,
    private readonly operatorToken: string,
    fetchFn: SlackbotV2Fetch,
    requestTimeoutMs: number
  ) {
    this.baseUrl = parseBrokerUrl(brokerUrl)
    this.fetchFn = fetchFn
    this.requestTimeoutMs = requestTimeoutMs
  }

  async status(): Promise<AutorotateStatus> {
    const payload = await this.request('GET', 'v1/status', this.observerToken)
    return selectFields(payload, STATUS_FIELDS) as AutorotateStatus
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
        options.observerToken?.trim() ?? '',
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
      if (!authorizedOperator(options, teamId, userId)) {
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
        waitUntil(
          respondWithStatus(client, form, options, fetchFn)
        )
        return ephemeralResponse('Checking the Codex account pool…')
      }

      if (command.kind === 'accounts') {
        const responseUrl = safeResponseUrl(form.get('response_url'), options)
        if (!responseUrl) {
          return ephemeralResponse('Slack did not provide a safe ephemeral response channel.')
        }
        waitUntil(respondWithAccounts(client, responseUrl, options, fetchFn))
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
            command,
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
  form: URLSearchParams,
  options: AutorotateSlackOptions,
  fetchFn: SlackbotV2Fetch
): Promise<void> {
  const responseUrl = safeResponseUrl(form.get('response_url'), options)
  if (!responseUrl) return
  try {
    const status = await client.status()
    await postSlackResponse(fetchFn, responseUrl, formatStatus(status), options.requestTimeoutMs)
  } catch (error) {
    safeCommandWarning(options.logger, 'slackbotv2_autorotate_status_failed', error)
    await postSlackResponse(
      fetchFn,
      responseUrl,
      'Autorotate status is temporarily unavailable.',
      options.requestTimeoutMs
    )
  }
}

async function respondWithAccounts(
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
      formatAccounts(accounts),
      options.requestTimeoutMs
    )
  } catch (error) {
    safeCommandWarning(options.logger, 'slackbotv2_autorotate_accounts_failed', error)
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
  command: Extract<ParsedCommand, { kind: 'login' }>
  fetchFn: SlackbotV2Fetch
  options: AutorotateSlackOptions
  pollIntervalMs: number
  responseUrl: string
}): Promise<void> {
  try {
    const reloginAccount = input.command.action === 'relogin' && input.command.account
      ? await findReloginAccount(input.client, input.command)
      : null
    const reloginExpectedEmail = input.command.action === 'relogin'
      ? input.command.expectedEmail ?? reloginAccount?.email ?? undefined
      : undefined
    const createInput: CreateEnrollmentInput = input.command.action === 'relogin'
      ? {
          action: 'relogin',
          owner: input.active.owner,
          ...(input.command.account ? { account: input.command.account } : {}),
          ...(reloginExpectedEmail ? { expected_email: reloginExpectedEmail } : {})
        }
      : {
          action: 'add',
          owner: input.active.owner,
          ...(input.command.label ? { label: input.command.label } : {}),
          ...(input.command.expectedEmail
            ? { expected_email: input.command.expectedEmail }
            : {})
        }
    const enrollment = await input.client.createEnrollment(createInput)
    const expectedLabel = createInput.action === 'add' ? createInput.label : createInput.account
    if (
      enrollment.action !== createInput.action
      || (expectedLabel !== undefined && enrollment.account_label !== expectedLabel)
    ) {
      throw new AutorotateError(
        'Autorotate returned an existing enrollment for this owner',
        409,
        'enrollment_already_active'
      )
    }
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
        ? formatDeviceCode(enrollment, reloginAccount)
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
    'Codex device login expired. Run `/autorotate add` or `/autorotate relogin` to start again.',
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
  | { kind: 'accounts' }
  | { kind: 'login'; action: 'add'; expectedEmail?: string; label?: string }
  | { kind: 'login'; action: 'relogin'; account?: string; expectedEmail?: string }
  | { kind: 'login_status' }
  | { kind: 'login_cancel' }

function parseCommand(text: string): ParsedCommand {
  const parts = text.trim().split(/\s+/).filter(Boolean)
  if (parts.length === 1 && parts[0]?.toLowerCase() === 'status') return { kind: 'status' }
  if (parts.length === 1 && parts[0]?.toLowerCase() === 'accounts') return { kind: 'accounts' }
  if (parts.length === 1 && parts[0]?.toLowerCase() === 'add') {
    return { kind: 'login', action: 'add' }
  }
  if (parts.length === 1 && parts[0]?.toLowerCase() === 'relogin') {
    return { kind: 'login', action: 'relogin' }
  }
  if (parts[0]?.toLowerCase() !== 'login') return { kind: 'help' }
  if (parts.length === 2 && parts[1]?.toLowerCase() === 'status') return { kind: 'login_status' }
  if (parts.length === 2 && parts[1]?.toLowerCase() === 'cancel') return { kind: 'login_cancel' }
  if (parts[1]?.toLowerCase() === 'relogin') {
    return parseReloginCommand(text) ?? { kind: 'help' }
  }
  if (parts.length > 3) return { kind: 'help' }

  const label = parts[1]
  const expectedEmail = parts[2]
  if (label && !/^[A-Za-z0-9._-]{1,64}$/.test(label)) return { kind: 'help' }
  if (expectedEmail && !safeEmail(expectedEmail)) return { kind: 'help' }
  return {
    kind: 'login',
    action: 'add',
    ...(label ? { label } : {}),
    ...(expectedEmail ? { expectedEmail } : {})
  }
}

function parseReloginCommand(
  text: string
): (Extract<ParsedCommand, { action: 'relogin'; kind: 'login' }> & { account: string }) | null {
  const match = /^\s*login\s+relogin\s+([\s\S]+?)\s*$/i.exec(text)
  if (!match?.[1]) return null
  const rest = match[1]
  let account: string
  let expectedEmail: string | undefined
  if (rest.startsWith('"')) {
    const end = jsonStringEnd(rest)
    if (end === null) return null
    try {
      const parsed: unknown = JSON.parse(rest.slice(0, end))
      if (typeof parsed !== 'string') return null
      account = parsed
    } catch {
      return null
    }
    const remainder = rest.slice(end).trim()
    if (remainder) {
      if (/\s/.test(remainder)) return null
      expectedEmail = remainder
    }
  } else {
    const parts = rest.split(/\s+/)
    if (parts.length > 2 || !parts[0] || !/^[A-Za-z0-9._-]{1,128}$/.test(parts[0])) {
      return null
    }
    account = parts[0]
    expectedEmail = parts[1]
  }
  if (!safeAccountLabel(account) || (expectedEmail && !safeEmail(expectedEmail))) return null
  return {
    kind: 'login',
    action: 'relogin',
    account,
    ...(expectedEmail ? { expectedEmail } : {})
  }
}

function jsonStringEnd(value: string): number | null {
  let escaped = false
  for (let index = 1; index < value.length; index += 1) {
    const character = value[index]
    if (escaped) {
      escaped = false
    } else if (character === '\\') {
      escaped = true
    } else if (character === '"') {
      return index + 1
    }
  }
  return null
}

function helpText(): string {
  return [
    '*Autorotate commands*',
    '• `/autorotate status` — redacted pool health and capacity',
    '• `/autorotate accounts` — account health, emails, and labels',
    '• `/autorotate add` — add the Codex account you authenticate',
    '• `/autorotate relogin` — repair the Codex account you authenticate'
  ].join('\n')
}

function formatStatus(status: AutorotateStatus): string {
  return [
    '*Codex account pool*',
    `Available: ${safeCount(status.available)} / ${safeCount(status.total)}`,
    `Healthy: ${safeCount(status.healthy)} · Leased: ${safeCount(status.leased)} · Limited: ${safeCount(status.limited)}`,
    `Login required: ${safeCount(status.login_required)} · Disabled: ${safeCount(status.disabled)} · Pending logins: ${safeCount(status.pending_enrollments)}`,
    safeTimestamp(status.next_available_at)
      ? `Next account available: ${safeTimestamp(status.next_available_at)}`
      : null
  ].filter(Boolean).join('\n')
}

function formatAccounts(accounts: readonly AutorotateAccount[]): string {
  if (accounts.length === 0) return '*Codex accounts*\nNo active accounts.'
  return [
    '*Codex accounts*',
    ...accounts.map(account => {
      const email = account.email ? escapeSlackText(account.email) : 'email unknown'
      const limitedUntil = safeTimestamp(account.limited_until)
      const condition = account.login_required
        ? `:red_circle: *UNUSABLE* — login required · status ${account.status}`
        : account.status !== 'enabled'
          ? `:red_circle: *UNUSABLE* — status ${account.status}`
          : limitedUntil
            ? `:large_yellow_circle: *LIMITED* — rate limited until ${limitedUntil}`
            : ':large_green_circle: *AVAILABLE*'
      return `• ${condition}\n  ${email} — \`${escapeSlackText(account.label)}\``
    })
  ].join('\n')
}

function formatDeviceCode(
  enrollment: StartEnrollmentResponse,
  reloginAccount: AutorotateAccount | null = null
): string {
  const verificationUrl = safeVerificationUrl(enrollment.verification_url)
  const userCode = safeUserCode(enrollment.user_code)
  if (!verificationUrl || !userCode) throw new AutorotateError('invalid device authorization')
  return [
    '*Codex device login*',
    reloginAccount
      ? `Reauthenticating \`${escapeSlackText(reloginAccount.label)}\` (${reloginAccount.email ? escapeSlackText(reloginAccount.email) : 'email unknown'}).`
      : null,
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

async function findReloginAccount(
  client: AutorotateClient,
  command: Extract<ParsedCommand, { action: 'relogin'; kind: 'login' }>
): Promise<AutorotateAccount> {
  if (!command.account) {
    throw new AutorotateError('relogin account was not specified')
  }
  const accounts = await client.accounts()
  const account = accounts.find(candidate => candidate.label === command.account)
  if (!account) {
    throw new AutorotateError('relogin account was not found', 404, 'account_not_found')
  }
  if (
    command.expectedEmail
    && account.email
    && command.expectedEmail.toLowerCase() !== account.email.toLowerCase()
  ) {
    throw new AutorotateError('relogin email did not match', 409, 'email_mismatch')
  }
  return account
}

function loginFailureText(error: unknown): string {
  if (error instanceof AutorotateError && error.code === 'account_not_found') {
    return 'No existing account matched that Codex login. Use `/autorotate add` to add it instead.'
  }
  if (error instanceof AutorotateError && error.code === 'account_busy') {
    return 'That Codex account is already being reauthenticated. Try again after the active login finishes.'
  }
  if (error instanceof AutorotateError && error.code === 'enrollment_already_active') {
    return 'Another Codex login is already active. Wait for it to finish, or try again after it expires.'
  }
  if (error instanceof AutorotateError && error.code === 'email_mismatch') {
    return 'The expected email does not match that Codex account.'
  }
  return 'Codex device login could not be started. Try again or ask an Autorotate operator.'
}

function formatTerminalEnrollment(enrollment: EnrollmentStatusResponse): string {
  switch (enrollment.status) {
    case 'completed':
      return completedEnrollmentText(enrollment)
    case 'cancelled':
      return 'Codex device login was cancelled.'
    case 'expired':
      return 'Codex device login expired. Run `/autorotate add` or `/autorotate relogin` to start again.'
    default:
      return 'Codex device login failed. Run `/autorotate add` or `/autorotate relogin` to try again.'
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
  if (
    typeof label !== 'string'
    || !safeAccountLabel(label)
    || (email !== null && !safeEmail(email))
    || !isAccountStatus(status)
    || (limitedUntil !== null && limitedUntil !== undefined && !safeTimestamp(limitedUntil))
    || typeof loginRequired !== 'boolean'
  ) {
    throw new AutorotateError('Autorotate returned invalid accounts')
  }
  return {
    label,
    email: email ?? null,
    status: status as AutorotateAccount['status'],
    limited_until: typeof limitedUntil === 'string' ? limitedUntil : null,
    login_required: loginRequired
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

function authorizedOperator(
  options: AutorotateSlackOptions,
  teamId: string,
  userId: string
): boolean {
  if (!SLACK_TEAM_ID_PATTERN.test(teamId) || !SLACK_MEMBER_ID_PATTERN.test(userId)) return false
  const teams = new Set(options.operatorSlackTeamIds ?? [])
  const users = new Set(options.operatorSlackUserIds ?? [])
  return teams.has(teamId) && users.has(userId)
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

function safeCount(value: number | undefined): string {
  return Number.isSafeInteger(value) && (value ?? -1) >= 0 ? String(value) : '—'
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
