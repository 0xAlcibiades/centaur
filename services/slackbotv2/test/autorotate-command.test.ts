import { createHmac } from 'node:crypto'
import { describe, expect, it } from 'bun:test'
import { createMemoryState } from '@chat-adapter/state-memory'
import type { Logger } from 'chat'
import { createAutorotateSlackCommandHandler } from '../src/autorotate-command'
import { createSlackbotV2 } from '../src/index'
import type { SlackbotV2Fetch } from '../src/types'

const SIGNING_SECRET = 'slack-signing-secret'
const TEAM_ID = 'T123456789'
const USER_ID = 'U123456789'
const OTHER_USER_ID = 'U987654321'
const RESPONSE_URL = 'https://hooks.slack.test/commands/response-secret'

type FetchCall = {
  body?: unknown
  method: string
  url: string
}

function startEnrollmentResponse(
  accountLabel: string,
  {
    action = 'relogin',
    enrollmentId = 'enr_abcdefgh',
    status = 'pending'
  }: {
    action?: 'add' | 'relogin'
    enrollmentId?: string
    status?: 'pending' | 'importing'
  } = {}
): Record<string, unknown> {
  return {
    enrollment_id: enrollmentId,
    action,
    account_label: accountLabel,
    ...(status === 'pending'
      ? {
          verification_url: 'https://auth.openai.com/codex/device',
          user_code: 'ABCD-EFGH'
        }
      : {}),
    expires_at: new Date(Date.now() + 60_000).toISOString(),
    status
  }
}

function enrollmentStatusResponse(
  status: 'pending' | 'importing' | 'completed' | 'failed' | 'cancelled' | 'expired',
  {
    accountLabel = 'team-codex',
    errorCode = status === 'failed' ? 'provider_login_failed' : null,
    enrollmentId = 'enr_abcdefgh'
  }: {
    accountLabel?: string
    errorCode?: string | null
    enrollmentId?: string
  } = {}
): Record<string, unknown> {
  return {
    enrollment_id: enrollmentId,
    status,
    expires_at: new Date(Date.now() + 60_000).toISOString(),
    account: status === 'completed'
      ? {
          label: accountLabel,
          email: 'person@example.test',
          status: 'enabled'
        }
      : null,
    error_code: errorCode
  }
}

function operatorAccount(
  overrides: Record<string, unknown> = {}
): Record<string, unknown> {
  return {
    label: 'team-codex',
    email: 'person@example.test',
    status: 'enabled',
    limited_until: null,
    limits_observed_at: null,
    login_required: false,
    reconciliation_required: false,
    active_writer: false,
    availability: 'available',
    unusable_reason: null,
    primary: null,
    secondary: null,
    next_available_at: null,
    ...overrides
  }
}

function traceConsent(
  overrides: Record<string, unknown> = {}
): Record<string, unknown> {
  return {
    data: {
      workspace_id: TEAM_ID,
      user_id: USER_ID,
      enabled: false,
      expires_at: null,
      revision: 1,
      ...overrides
    }
  }
}

async function traceConfirmation(
  handler: ReturnType<typeof createAutorotateSlackCommandHandler>,
  duration = '1h'
): Promise<string> {
  const response = await handleAndWait(handler, commandRequest(`trace on ${duration}`))
  const body = await response.json() as { text: string }
  const token = /`\/autorotate trace confirm ([^`\s]+)`/.exec(body.text)?.[1]
  if (!token) throw new Error('trace preview did not return a confirmation token')
  return token
}

function fleetReport(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    generated_at: new Date().toISOString(),
    active_sessions: [{
      account_label: 'team-codex',
      account_email: 'person@example.test',
      consumer_fingerprint: `sha256:${'a'.repeat(64)}`,
      client_version: '0.4.23',
      created_at: new Date(Date.now() - 60_000).toISOString(),
      last_heartbeat_at: new Date(Date.now() - 5_000).toISOString(),
      expires_at: new Date(Date.now() + 60_000).toISOString()
    }],
    terminal_leases: {
      clean: 2,
      expired: 1,
      expired_unreleased: 0,
      unusable: 1,
      unknown_legacy: 0,
      other: 0,
      total: 4
    },
    ...overrides
  }
}

function maintenanceStatus(
  overrides: Record<string, unknown> = {}
): Record<string, unknown> {
  return {
    mode: 'serving',
    control_epoch: 7,
    changed_at: new Date().toISOString(),
    active_leases: 0,
    active_quota_probes: 0,
    active_enrollments: 0,
    drain_clean_terminals: 0,
    drain_expired_terminals: 0,
    drain_other_terminals: 0,
    quiescent: true,
    ...overrides
  }
}

function maintenanceOperationResult(
  operation: 'drain' | 'resume',
  status: Record<string, unknown>
): Record<string, unknown> {
  return { operation, status }
}

function logger(logs: Array<{ data?: unknown; event: string }>): Logger {
  const capture = {
    debug: (event: string, data?: unknown) => logs.push({ event, data }),
    info: (event: string, data?: unknown) => logs.push({ event, data }),
    warn: (event: string, data?: unknown) => logs.push({ event, data }),
    error: (event: string, data?: unknown) => logs.push({ event, data }),
    child: () => capture
  }
  return capture
}

function commandRequest(
  text: string,
  {
    channelId = 'C123456789',
    responseUrl = RESPONSE_URL,
    signatureValid = true,
    teamId = TEAM_ID,
    userId = USER_ID,
    timestamp = Math.floor(Date.now() / 1000)
  }: {
    channelId?: string
    responseUrl?: string
    signatureValid?: boolean
    teamId?: string
    userId?: string
    timestamp?: number
  } = {}
): Request {
  const body = new URLSearchParams({
    channel_id: channelId,
    command: '/autorotate',
    response_url: responseUrl,
    team_id: teamId,
    text,
    user_id: userId
  }).toString()
  const timestampHeader = String(timestamp)
  const signature = createHmac('sha256', SIGNING_SECRET)
    .update(`v0:${timestampHeader}:${body}`)
    .digest('hex')
  return new Request('https://bot.example.test/api/slack/commands', {
    method: 'POST',
    headers: {
      'content-type': 'application/x-www-form-urlencoded',
      'x-slack-request-timestamp': timestampHeader,
      'x-slack-signature': `v0=${signatureValid ? signature : '0'.repeat(64)}`
    },
    body
  })
}

function testHandler(
  fetchFn: SlackbotV2Fetch,
  logs: Array<{ data?: unknown; event: string }> = [],
  requestTimeoutMs = 1_000,
  authorization: {
    operatorSlackTeamIds?: readonly string[]
    operatorSlackUserIds?: readonly string[]
  } = {}
) {
  return createAutorotateSlackCommandHandler({
    apiKey: 'slackbot-api-secret',
    traceConsentApiKey: 'trace-consent-api-secret',
    apiUrl: 'https://centaur-api.example.test',
    brokerUrl: 'https://autorotate.example.test/broker/',
    fetch: fetchFn,
    logger: logger(logs),
    controlToken: 'control-secret',
    operatorSlackTeamIds: authorization.operatorSlackTeamIds ?? [TEAM_ID],
    operatorSlackUserIds: authorization.operatorSlackUserIds ?? [`${TEAM_ID}:${USER_ID}`],
    operatorToken: 'operator-secret',
    pollIntervalMs: 1,
    requestTimeoutMs,
    responseUrlHosts: ['hooks.slack.test'],
    signingSecret: SIGNING_SECRET
  })
}

async function handleAndWait(
  handler: ReturnType<typeof testHandler>,
  request: Request
): Promise<Response> {
  const waits: Promise<unknown>[] = []
  const response = await handler.handle(request, promise => waits.push(promise))
  expect(response).not.toBeNull()
  await Promise.all(waits)
  return response!
}

describe('Autorotate Slack command', () => {
  it('is intercepted before the durable Centaur session path', async () => {
    const calls: string[] = []
    const fetchFn: SlackbotV2Fetch = async (input, init) => {
      const url = String(input)
      calls.push(url)
      if (url.endsWith('/v1/operator/accounts')) {
        return Response.json({
          accounts: [operatorAccount()]
        })
      }
      return new Response('', { status: 200 })
    }
    const bot = createSlackbotV2({
      apiUrl: 'https://centaur-api.example.test',
      apiKey: 'slackbot-api-secret',
      autorotateOperatorToken: 'operator-secret',
      autorotateSlackResponseUrlHosts: ['hooks.slack.test'],
      autorotateSlackTeamIds: [TEAM_ID],
      autorotateUrl: 'https://autorotate.example.test/broker/',
      botToken: 'xoxb-test',
      fetch: fetchFn,
      signingSecret: SIGNING_SECRET,
      state: createMemoryState()
    })
    const waits: Promise<unknown>[] = []

    const response = await bot.app.request(
      '/api/slack/commands',
      commandRequest('status'),
      {},
      {
        waitUntil: promise => {
          waits.push(promise)
        },
        passThroughOnException() {},
        props: {}
      }
    )
    await Promise.all(waits)

    expect(response.status).toBe(200)
    expect(calls.some(url => url.startsWith('https://centaur-api.example.test'))).toBe(false)
    expect(calls.some(url => url.endsWith('/v1/operator/accounts'))).toBe(true)
  })

  it('rejects invalid Slack signatures before any broker request', async () => {
    const calls: FetchCall[] = []
    const handler = testHandler(async input => {
      calls.push({ method: 'GET', url: String(input) })
      return Response.json({})
    })

    const response = await handleAndWait(
      handler,
      commandRequest('status', { signatureValid: false })
    )

    expect(response.status).toBe(401)
    expect(calls).toEqual([])
  })

  it('allows every valid member of the configured workspace and rejects other workspaces', async () => {
    let brokerRequests = 0
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      if (url.endsWith('/v1/operator/accounts')) {
        brokerRequests += 1
        return Response.json({ accounts: [operatorAccount()] })
      }
      if (url === RESPONSE_URL) return new Response('', { status: 200 })
      throw new Error(`unexpected request: ${url} ${init?.method ?? 'GET'}`)
    })

    const allowed = await handleAndWait(
      handler,
      commandRequest('status', { userId: OTHER_USER_ID })
    )
    expect(await allowed.json()).toMatchObject({ text: 'Checking Codex account status…' })
    expect(brokerRequests).toBe(1)

    const denied = await handleAndWait(
      handler,
      commandRequest('status', { teamId: 'T999999999', userId: OTHER_USER_ID })
    )

    expect(denied.status).toBe(200)
    expect(await denied.json()).toEqual({
      response_type: 'ephemeral',
      text: 'You are not authorized to operate the Codex account pool.'
    })
    expect(brokerRequests).toBe(1)
  })

  it('renders only the versioned redacted fleet document for every allowed workspace member', async () => {
    const posts: string[] = []
    const session = (fleetReport().active_sessions as Array<Record<string, unknown>>)[0]!
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      if (url.endsWith('/v1/fleet')) {
        expect(new Headers(init?.headers).get('authorization')).toBe('Bearer control-secret')
        expect(init?.cache).toBe('no-store')
        return Response.json(fleetReport({
          active_sessions: [{
            ...session,
            lease_id: 'must-not-render',
            client_version: null,
            provider_subject: 'must-not-render'
          }]
        }))
      }
      if (url === RESPONSE_URL) {
        posts.push((JSON.parse(String(init?.body)) as { text: string }).text)
        return new Response('', { status: 200 })
      }
      throw new Error(`unexpected request: ${url}`)
    })

    const response = await handleAndWait(handler, commandRequest('fleet', { userId: OTHER_USER_ID }))

    expect(await response.json()).toMatchObject({
      response_type: 'ephemeral',
      text: 'Checking redacted Codex fleet status…'
    })
    expect(posts).toHaveLength(1)
    expect(posts[0]).toContain(`sha256:${'a'.repeat(64)}`)
    expect(posts[0]).toContain('client version: unknown')
    expect(posts[0]).toContain('expired unreleased 0')
    expect(posts[0]).not.toContain('must-not-render')
    expect(posts[0]).not.toContain('lease_id')
  })

  it('allows workspace-wide maintenance reads but fails drain closed for a non-operator', async () => {
    const calls: string[] = []
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      calls.push(url)
      if (url.endsWith('/v1/maintenance')) return Response.json(maintenanceStatus())
      if (url === RESPONSE_URL) return new Response('', { status: 200 })
      throw new Error(`unexpected request: ${url} ${init?.method ?? 'GET'}`)
    })

    const read = await handleAndWait(
      handler,
      commandRequest('maintenance status', { userId: OTHER_USER_ID })
    )
    expect(await read.json()).toMatchObject({ text: 'Checking Autorotate maintenance status…' })
    expect(calls).toContain('https://autorotate.example.test/broker/v1/maintenance')

    const write = await handleAndWait(
      handler,
      commandRequest('maintenance drain', { userId: OTHER_USER_ID })
    )
    expect(await write.json()).toEqual({
      response_type: 'ephemeral',
      text: 'You are not authorized to change Autorotate maintenance.'
    })
    expect(calls.filter(url => url.endsWith('/v1/maintenance/drain'))).toEqual([])
  })

  it('does not let the same Slack user ID inherit maintenance write access in another workspace', async () => {
    const otherTeamId = 'T987654321'
    const calls: string[] = []
    const handler = testHandler(async input => {
      const url = String(input)
      calls.push(url)
      if (url.endsWith('/v1/maintenance')) return Response.json(maintenanceStatus())
      if (url === RESPONSE_URL) return new Response('', { status: 200 })
      throw new Error(`unexpected request: ${url}`)
    }, [], 1_000, {
      operatorSlackTeamIds: [TEAM_ID, otherTeamId],
      operatorSlackUserIds: [`${TEAM_ID}:${USER_ID}`]
    })

    const read = await handleAndWait(
      handler,
      commandRequest('maintenance status', { teamId: otherTeamId, userId: USER_ID })
    )
    expect(await read.json()).toMatchObject({ text: 'Checking Autorotate maintenance status…' })

    const write = await handleAndWait(
      handler,
      commandRequest('maintenance drain', { teamId: otherTeamId, userId: USER_ID })
    )
    expect(await write.json()).toMatchObject({ text: 'You are not authorized to change Autorotate maintenance.' })
    expect(calls.some(url => url.includes('/v1/maintenance/requests/'))).toBe(false)
  })

  it('replays a deterministic maintenance request ID and resumes with the exact drain epoch', async () => {
    const requests: Array<{ body: unknown; id: string | null; url: string }> = []
    let lookupCount = 0
    let statusReads = 0
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      if (url.includes('/v1/maintenance/requests/')) {
        lookupCount += 1
        return lookupCount === 1
          ? Response.json({ error: { code: 'not_found' } }, { status: 404 })
          : Response.json(maintenanceOperationResult('resume', maintenanceStatus({ control_epoch: 9 })))
      }
      if (url.endsWith('/v1/maintenance')) {
        statusReads += 1
        return Response.json(maintenanceStatus({ mode: 'draining', control_epoch: 8, drain_completion: 'drained' }))
      }
      if (url.endsWith('/v1/maintenance/resume')) {
        requests.push({
          body: JSON.parse(String(init?.body)),
          id: new Headers(init?.headers).get('x-request-id'),
          url
        })
        return Response.json(maintenanceStatus({ control_epoch: 9 }))
      }
      if (url === RESPONSE_URL) return new Response('', { status: 200 })
      throw new Error(`unexpected request: ${url}`)
    })
    const timestamp = Math.floor(Date.now() / 1000)

    await handleAndWait(handler, commandRequest('maintenance resume', { timestamp }))
    await handleAndWait(handler, commandRequest('maintenance resume', { timestamp }))

    expect(requests).toHaveLength(1)
    expect(requests.map(request => request.body)).toEqual([{ expected_drain_epoch: 8 }])
    expect(new Set(requests.map(request => request.id))).toHaveLength(1)
    expect(requests[0]?.id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/)
    expect(statusReads).toBe(1)
  })

  it('retries a transient drain with the same request ID only after lookup returns absent', async () => {
    const writes: Array<{ body: unknown; id: string | null }> = []
    let lookupCount = 0
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      if (url.includes('/v1/maintenance/requests/')) {
        lookupCount += 1
        return Response.json({ error: { code: 'not_found' } }, { status: 404 })
      }
      if (url.endsWith('/v1/maintenance')) return Response.json(maintenanceStatus())
      if (url.endsWith('/v1/maintenance/drain')) {
        writes.push({ body: JSON.parse(String(init?.body)), id: new Headers(init?.headers).get('x-request-id') })
        return writes.length === 1
          ? Response.json({ error: { code: 'unavailable' } }, { status: 503 })
          : Response.json(maintenanceStatus({ mode: 'draining', control_epoch: 8, drain_completion: 'pending', quiescent: false }))
      }
      if (url === RESPONSE_URL) return new Response('', { status: 200 })
      throw new Error(`unexpected request: ${url}`)
    })

    await handleAndWait(handler, commandRequest('maintenance drain'))

    expect(writes).toHaveLength(2)
    expect(new Set(writes.map(write => JSON.stringify(write.body)))).toEqual(new Set(['{"expected_control_epoch":7,"reason_code":"slack_maintenance"}']))
    expect(new Set(writes.map(write => write.id))).toHaveLength(1)
    expect(lookupCount).toBe(2)
  })

  it('returns the stored drain result for a duplicate delivery without rebuilding against the live epoch', async () => {
    let lookupCount = 0
    let statusReads = 0
    let writes = 0
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      if (url.includes('/v1/maintenance/requests/')) {
        lookupCount += 1
        return lookupCount === 1
          ? Response.json({ error: { code: 'not_found' } }, { status: 404 })
          : Response.json(maintenanceOperationResult('drain', maintenanceStatus({ mode: 'draining', control_epoch: 8, drain_completion: 'pending', quiescent: false })))
      }
      if (url.endsWith('/v1/maintenance')) {
        statusReads += 1
        return Response.json(maintenanceStatus(statusReads === 1 ? {} : { mode: 'draining', control_epoch: 8 }))
      }
      if (url.endsWith('/v1/maintenance/drain')) {
        writes += 1
        expect(JSON.parse(String(init?.body))).toEqual({ expected_control_epoch: 7, reason_code: 'slack_maintenance' })
        return Response.json(maintenanceStatus({ mode: 'draining', control_epoch: 8, drain_completion: 'pending', quiescent: false }))
      }
      if (url === RESPONSE_URL) return new Response('', { status: 200 })
      throw new Error(`unexpected request: ${url}`)
    })
    const timestamp = Math.floor(Date.now() / 1000)

    await handleAndWait(handler, commandRequest('maintenance drain', { timestamp }))
    await handleAndWait(handler, commandRequest('maintenance drain', { timestamp }))

    expect(writes).toBe(1)
    expect(statusReads).toBe(1)
    expect(lookupCount).toBe(2)
  })

  it('resolves a maintenance body-hash conflict from the durable request result without another write', async () => {
    let lookupCount = 0
    let writes = 0
    const handler = testHandler(async input => {
      const url = String(input)
      if (url.includes('/v1/maintenance/requests/')) {
        lookupCount += 1
        return lookupCount === 1
          ? Response.json({ error: { code: 'not_found' } }, { status: 404 })
          : Response.json(maintenanceOperationResult('drain', maintenanceStatus({ mode: 'draining', control_epoch: 8, drain_completion: 'pending', quiescent: false })))
      }
      if (url.endsWith('/v1/maintenance')) return Response.json(maintenanceStatus())
      if (url.endsWith('/v1/maintenance/drain')) {
        writes += 1
        return Response.json({ error: { code: 'maintenance_request_hash_mismatch' } }, { status: 409 })
      }
      if (url === RESPONSE_URL) return new Response('', { status: 200 })
      throw new Error(`unexpected request: ${url}`)
    })

    await handleAndWait(handler, commandRequest('maintenance drain'))

    expect(writes).toBe(1)
    expect(lookupCount).toBe(2)
  })

  it('recovers an ambiguous drain timeout from the same durable request ID without rebuilding the body', async () => {
    let lookupCount = 0
    let writes = 0
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      if (url.includes('/v1/maintenance/requests/')) {
        lookupCount += 1
        return lookupCount === 1
          ? Response.json({ error: { code: 'not_found' } }, { status: 404 })
          : Response.json(maintenanceOperationResult('drain', maintenanceStatus({ mode: 'draining', control_epoch: 8, drain_completion: 'pending', quiescent: false })))
      }
      if (url.endsWith('/v1/maintenance')) return Response.json(maintenanceStatus())
      if (url.endsWith('/v1/maintenance/drain')) {
        writes += 1
        return new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener('abort', () => reject(new Error('aborted')), { once: true })
        })
      }
      if (url === RESPONSE_URL) return new Response('', { status: 200 })
      throw new Error(`unexpected request: ${url}`)
    }, [], 5)

    await handleAndWait(handler, commandRequest('maintenance drain'))

    expect(writes).toBe(1)
    expect(lookupCount).toBe(2)
  })

  it('advertises trace consent commands in ephemeral help', async () => {
    const handler = testHandler(async () => {
      throw new Error('must not fetch')
    })

    const response = await handleAndWait(handler, commandRequest(''))
    const body = await response.json() as { response_type: string; text: string }

    expect(body.response_type).toBe('ephemeral')
    expect(body.text).toContain('/autorotate status')
    expect(body.text).toContain('/autorotate login')
    expect(body.text).toContain('/autorotate trace status')
    expect(body.text).toContain('/autorotate trace on [duration]')
    expect(body.text).toContain('/autorotate trace confirm <token>')
    expect(body.text).toContain('/autorotate trace off')
    expect(body.text).toContain('default: 1h; max: 24h')
    expect(body.text).toContain('source, Codex version, pseudonymous execution/thread/account IDs')
    expect(body.text).toContain('transport kind/outcome/status class')
    expect(body.text).not.toContain('model')
    expect(body.text).toContain('never prompts, responses, tool arguments, or tool output')
    expect(body.text).toContain('Only SSH-key holders can read traces')
    expect(body.text).toContain('Producer spool: 24h; archive: 30d; snapshots can survive up to 45d')
    expect(body.text).toContain('Revoking or expiry fences future intake; it does not delete retained data')
    expect(body.text).not.toContain('/autorotate accounts')
    expect(body.text).not.toContain('/autorotate add')
    expect(body.text).not.toContain('/autorotate relogin')
    expect(body.text).not.toContain('•')
  })

  it('reads the invoking member trace consent with Slackbot API bearer auth and no-store responses', async () => {
    const calls: FetchCall[] = []
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      const body = init?.body ? JSON.parse(String(init.body)) : undefined
      calls.push({ body, method: init?.method ?? 'GET', url })
      if (url.includes('/api/v1/slack_trace_consents/')) {
        expect(url).toBe(`https://centaur-api.example.test/api/v1/slack_trace_consents/${TEAM_ID}/${OTHER_USER_ID}`)
        expect(new Headers(init?.headers).get('authorization')).toBe('Bearer trace-consent-api-secret')
        expect(init?.cache).toBe('no-store')
        return Response.json(traceConsent({ user_id: OTHER_USER_ID }))
      }
      return new Response('', { status: 200 })
    })

    const response = await handleAndWait(
      handler,
      commandRequest('trace status', { userId: OTHER_USER_ID })
    )

    expect(response.headers.get('cache-control')).toBe('no-store')
    expect(await response.json()).toEqual({
      response_type: 'ephemeral',
      text: 'Trace consent is durably off: its input fence rejects new trace data and no exact sandbox drain is pending.'
    })
    expect(calls.some(call => call.url === RESPONSE_URL)).toBe(false)
    expect(calls).toContainEqual({
      method: 'GET',
      url: `https://centaur-api.example.test/api/v1/slack_trace_consents/${TEAM_ID}/${OTHER_USER_ID}`
    })
  })

  it('reviews trace consent before issuing an activation token', async () => {
    const handler = testHandler(async input => {
      if (String(input).includes('/api/v1/slack_trace_consents/')) return Response.json(traceConsent())
      throw new Error('unexpected trace review request')
    })

    const response = await handleAndWait(handler, commandRequest('trace on 90m'))
    const body = await response.json() as { response_type: string; text: string }

    expect(body.response_type).toBe('ephemeral')
    expect(body.text).toContain('This would set metadata collection to 90m')
    expect(body.text).toContain('Codex version')
    expect(body.text).toContain('transport kind/outcome/status class')
    expect(body.text).toContain('`/autorotate trace confirm ')
  })

  it('enables confirmed trace metadata collection for at most 24 hours and reports expiry ephemerally', async () => {
    const calls: FetchCall[] = []
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      const body = init?.body ? JSON.parse(String(init.body)) : undefined
      calls.push({ body, method: init?.method ?? 'GET', url })
      if (url.includes('/api/v1/slack_trace_consents/')) {
        if (init?.method === 'GET') return Response.json(traceConsent())
        expect(new Headers(init?.headers).get('authorization')).toBe('Bearer trace-consent-api-secret')
        expect(body).toEqual({ data: { expected_revision: 1, duration_seconds: 5_400 } })
        const expiresAt = new Date(Date.now() + 90 * 60_000).toISOString()
        return Response.json(traceConsent({
          enabled: true,
          expires_at: expiresAt,
          revision: 2
        }))
      }
      return new Response('', { status: 200 })
    })

    const token = await traceConfirmation(handler, '90m')
    const response = await handleAndWait(handler, commandRequest(`trace confirm ${token}`))

    const body = await response.json() as { response_type: string; text: string }
    expect(body.response_type).toBe('ephemeral')
    expect(body.text).toContain('Slack trace collection is on until')
    expect(body.text).toContain('it expires automatically')
    expect(body.text).toContain('source, Codex version, pseudonymous execution/thread/account IDs')
    expect(body.text).toContain('transport kind/outcome/status class')
    expect(body.text).not.toContain('model')
    expect(body.text).toContain('Never prompts, responses, tool arguments, or tool output')
    expect(body.text).toContain('Only SSH-key holders can read traces')
    expect(body.text).toContain('Producer spool: 24h; archive: 30d; snapshots can survive up to 45d')
    expect(body.text).toContain('Revoking or expiry fences future intake; it does not delete retained data')
    const upstream = calls.find(call => call.method === 'PUT')
    expect(upstream?.method).toBe('PUT')
  })

  it('fails closed for cold and actor-mismatched trace confirmations', async () => {
    const calls: FetchCall[] = []
    const handler = testHandler(async (input, init) => {
      calls.push({ method: init?.method ?? 'GET', url: String(input) })
      return Response.json(traceConsent())
    })
    const token = await traceConfirmation(handler, '90m')

    for (const request of [
      commandRequest('trace confirm 90m'),
      commandRequest(`trace confirm ${token}`, { userId: OTHER_USER_ID })
    ]) {
      const response = await handleAndWait(handler, request)
      expect(await response.json()).toMatchObject({
        text: 'Your Slack trace setting could not be confirmed. Check `/autorotate trace status`.'
      })
    }
    expect(calls).toHaveLength(1)
    expect(calls[0]).toMatchObject({ method: 'GET' })
  })

  it('defaults trace consent to one hour', async () => {
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      if (url.includes('/api/v1/slack_trace_consents/')) {
        if (init?.method === 'GET') return Response.json(traceConsent())
        const body = JSON.parse(String(init?.body)) as { data: { expires_at: string } }
        const remainingMs = Date.parse(body.data.expires_at) - Date.now()
        expect(remainingMs).toBeGreaterThan(59 * 60_000)
        expect(remainingMs).toBeLessThanOrEqual(60 * 60_000)
        return Response.json(traceConsent({
          enabled: true,
          expires_at: body.data.expires_at
        }))
      }
      return new Response('', { status: 200 })
    })

    const token = await traceConfirmation(handler)
    await handleAndWait(handler, commandRequest(`trace confirm ${token}`))
  })

  it('rejects invalid trace duration and target selectors before calling the API', async () => {
    const calls: FetchCall[] = []
    const handler = testHandler(async (input, init) => {
      calls.push({ method: init?.method ?? 'GET', url: String(input) })
      return new Response('', { status: 200 })
    })

    for (const command of ['trace on 25h', `trace off ${OTHER_USER_ID}`]) {
      const response = await handleAndWait(handler, commandRequest(command))
      expect(await response.json()).toMatchObject({ text: expect.stringContaining('Autorotate commands') })
    }
    expect(calls).toEqual([])
  })

  it('awaits the trace revocation fence before returning a successful response', async () => {
    let releaseDelete: (() => void) | undefined
    const deleteReleased = new Promise<void>(resolve => {
      releaseDelete = resolve
    })
    let markDeleteStarted: (() => void) | undefined
    const deleteStarted = new Promise<void>(resolve => {
      markDeleteStarted = resolve
    })
    const waits: Promise<unknown>[] = []
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      if (url.includes('/api/v1/slack_trace_consents/')) {
        expect(init?.method).toBe('DELETE')
        expect(new Headers(init?.headers).get('idempotency-key')).toMatch(/^slack-trace-off:[0-9a-f]{64}$/)
        markDeleteStarted?.()
        await deleteReleased
        return Response.json(traceConsent())
      }
      throw new Error(`unexpected request: ${url}`)
    })

    const responsePromise = handler.handle(commandRequest('trace off'), promise => waits.push(promise))
    await deleteStarted
    let returned = false
    void responsePromise.then(() => {
      returned = true
    })
    await Promise.resolve()

    expect(returned).toBe(false)
    expect(waits).toEqual([])
    releaseDelete?.()
    const response = await responsePromise
    expect(await response?.json()).toEqual({
      response_type: 'ephemeral',
      text: 'Trace consent is durably off: its input fence rejects new trace data and no exact sandbox drain is pending.'
    })
  })

  it('reports durable trace revocation with a pending Kubernetes drain', async () => {
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      if (url.includes('/api/v1/slack_trace_consents/')) {
        return Response.json(traceConsent({ drain_pending: true }))
      }
      throw new Error(`unexpected request: ${url}`)
    })

    const response = await handleAndWait(handler, commandRequest('trace off'))

    expect(await response.json()).toMatchObject({
      text: 'Trace consent is durably off: its input fence rejects new trace data. An exact sandbox drain is pending, so its sidecar is not yet confirmed gone. Check `/autorotate trace status`.'
    })
  })

  it('shows pending Kubernetes drain state in trace status', async () => {
    const handler = testHandler(async input => {
      if (String(input).includes('/api/v1/slack_trace_consents/')) {
        return Response.json(traceConsent({ drain_pending: true }))
      }
      throw new Error('trace status must return directly to Slack')
    })

    const response = await handleAndWait(handler, commandRequest('trace status'))

    expect(await response.json()).toMatchObject({
      text: 'Trace consent is durably off: its input fence rejects new trace data. An exact sandbox drain is pending, so its sidecar is not yet confirmed gone. Check `/autorotate trace status`.'
    })
  })

  it('reports trace revoke failures and timeouts without logging the response URL or API credential', async () => {
    for (const apiFailure of ['http', 'timeout'] as const) {
      const calls: FetchCall[] = []
      const logs: Array<{ data?: unknown; event: string }> = []
      const handler = testHandler(async (input, init) => {
        const url = String(input)
        calls.push({ method: init?.method ?? 'GET', url })
        if (url.includes('/api/v1/slack_trace_consents/')) {
          if (apiFailure === 'http') {
            return Response.json({ error: { code: 'upstream_unavailable' } }, { status: 503 })
          }
          return new Promise<Response>((_resolve, reject) => {
            init?.signal?.addEventListener('abort', () => reject(new Error('aborted')), { once: true })
          })
        }
        throw new Error(`unexpected request: ${url}`)
      }, logs, 5)

      const response = await handleAndWait(handler, commandRequest('trace off'))

      expect(await response.json()).toMatchObject({
        text: 'Trace collection revoke could not be confirmed. Check `/autorotate trace status`.'
      })
      expect(calls.filter(call => call.url.includes('/api/v1/slack_trace_consents/'))).toHaveLength(1)
      expect(calls.some(call => call.url === RESPONSE_URL)).toBe(false)
      expect(JSON.stringify(logs)).not.toContain('response-secret')
      expect(JSON.stringify(logs)).not.toContain('trace-consent-api-secret')
    }
  })

  it('bounds synchronous trace GET and DELETE to one two-second attempt', async () => {
    const originalSetTimeout = globalThis.setTimeout
    const originalClearTimeout = globalThis.clearTimeout
    const timeouts: number[] = []
    globalThis.setTimeout = ((callback: () => void, delay?: number) => {
      timeouts.push(delay ?? 0)
      queueMicrotask(callback)
      return 0 as unknown as ReturnType<typeof setTimeout>
    }) as typeof setTimeout
    globalThis.clearTimeout = (() => {}) as typeof clearTimeout
    try {
      for (const [command, method, failureText] of [
        ['trace status', 'GET', 'Your Slack trace setting is temporarily unavailable. Check `/autorotate trace status`.'],
        ['trace off', 'DELETE', 'Trace collection revoke could not be confirmed. Check `/autorotate trace status`.']
      ] as const) {
        let attempts = 0
        const handler = testHandler(async (input, init) => {
          if (!String(input).includes('/api/v1/slack_trace_consents/')) {
            throw new Error(`unexpected request: ${String(input)}`)
          }
          attempts += 1
          expect(init?.method).toBe(method)
          return new Promise<Response>((_resolve, reject) => {
            init?.signal?.addEventListener('abort', () => reject(new Error('aborted')), { once: true })
          })
        }, [], 2_000)
        const startedAt = performance.now()

        const response = await handleAndWait(handler, commandRequest(command))

        expect(performance.now() - startedAt).toBeLessThan(3_000)
        expect(attempts).toBe(1)
        expect(await response.json()).toMatchObject({ text: failureText })
      }
      expect(timeouts).toEqual([2_000, 2_000])
    } finally {
      globalThis.setTimeout = originalSetTimeout
      globalThis.clearTimeout = originalClearTimeout
    }
  })

  it('uses a stable API idempotency key for duplicate trace off deliveries', async () => {
    const effects = new Set<string>()
    const idempotencyKeys: string[] = []
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      if (url.includes('/api/v1/slack_trace_consents/')) {
        const key = new Headers(init?.headers).get('idempotency-key')
        expect(key).toBeString()
        idempotencyKeys.push(key!)
        effects.add(key!)
        return Response.json(traceConsent())
      }
      throw new Error(`unexpected request: ${url}`)
    })
    const timestamp = Math.floor(Date.now() / 1000)
    const request = () => commandRequest('trace off', { timestamp })

    await handleAndWait(handler, request())
    await handleAndWait(handler, request())

    expect(new Set(idempotencyKeys).size).toBe(1)
    expect(effects.size).toBe(1)
  })

  it('does not require or use a response URL for trace revocation', async () => {
    const calls: FetchCall[] = []
    const handler = testHandler(async (input, init) => {
      calls.push({ method: init?.method ?? 'GET', url: String(input) })
      if (String(input).includes('/api/v1/slack_trace_consents/')) return Response.json(traceConsent())
      throw new Error('trace revocation must return directly to Slack')
    })

    const response = await handleAndWait(
      handler,
      commandRequest('trace off', { responseUrl: 'https://attacker.example.test/response-secret' })
    )

    expect(await response.json()).toEqual({
      response_type: 'ephemeral',
      text: 'Trace consent is durably off: its input fence rejects new trace data and no exact sandbox drain is pending.'
    })
    expect(calls).toHaveLength(1)
    expect(calls[0]).toMatchObject({ method: 'DELETE' })
  })

  it('uses one confirmation token as the durable idempotency key for duplicate trace grants', async () => {
    const expiries: string[] = []
    const handler = testHandler(async (input, init) => {
      if (String(input).includes('/api/v1/slack_trace_consents/')) {
        if (init?.method === 'GET') return Response.json(traceConsent())
        const body = JSON.parse(String(init?.body)) as { data: { duration_seconds: number } }
        expiries.push(String(body.data.duration_seconds))
        return Response.json(traceConsent({ enabled: true, expires_at: new Date().toISOString(), revision: 1 }))
      }
      throw new Error('trace command must return directly to Slack')
    })
    const token = await traceConfirmation(handler, '90m')

    await handleAndWait(handler, commandRequest(`trace confirm ${token}`))
    await handleAndWait(handler, commandRequest(`trace confirm ${token}`))

    expect(expiries).toHaveLength(2)
    expect(expiries[1]).toBe(expiries[0])
  })

  it('accepts an expired disabled consent and renders it as off', async () => {
    const handler = testHandler(async input => {
      if (String(input).includes('/api/v1/slack_trace_consents/')) {
        return Response.json(traceConsent({ expires_at: new Date(Date.now() - 60_000).toISOString() }))
      }
      throw new Error('trace command must return directly to Slack')
    })

    const response = await handleAndWait(handler, commandRequest('trace status'))
    expect(await response.json()).toMatchObject({ text: expect.stringContaining('Trace consent is durably off') })
  })

  it('returns account status through the operator token', async () => {
    const calls: FetchCall[] = []
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      const body = init?.body ? JSON.parse(String(init.body)) : undefined
      calls.push({ body, method: init?.method ?? 'GET', url })
      if (url.endsWith('/v1/operator/accounts')) {
        expect(new Headers(init?.headers).get('authorization')).toBe('Bearer operator-secret')
        return Response.json({
          accounts: [operatorAccount()]
        })
      }
      return new Response('', { status: 200 })
    })

    const response = await handleAndWait(handler, commandRequest('status'))

    expect(response.status).toBe(200)
    const slackCall = calls.find(call => call.url === RESPONSE_URL)
    expect(slackCall).toBeDefined()
    expect(JSON.stringify(slackCall?.body)).toContain('Codex accounts: 1 usable / 1')
    expect(calls.some(call => call.url.includes('/v1/status'))).toBe(false)
  })

  it('starts login from a channel, sends the code only ephemerally, and replaces it on completion', async () => {
    const calls: FetchCall[] = []
    const logs: Array<{ data?: unknown; event: string }> = []
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      const body = init?.body ? JSON.parse(String(init.body)) : undefined
      calls.push({ body, method: init?.method ?? 'GET', url })
      if (url.endsWith('/v1/operator/enrollments')) {
        expect(new Headers(init?.headers).get('authorization')).toBe('Bearer operator-secret')
        expect(body).toEqual({
          action: 'relogin',
          owner: `slack:${TEAM_ID}:${OTHER_USER_ID}`
        })
        const start: Record<string, unknown> = {
          ...startEnrollmentResponse('team-codex'),
          auth_json: { refresh_token: 'must-not-render' }
        }
        delete start.account_label
        return Response.json(start, { status: 201 })
      }
      if (url.endsWith('/v1/operator/enrollments/enr_abcdefgh')) {
        return Response.json({
          ...enrollmentStatusResponse('completed'),
          account: {
            label: 'team-codex',
            email: 'person@example.test',
            status: 'enabled',
            auth_json: { refresh_token: 'must-not-render' }
          }
        })
      }
      return new Response('', { status: 200 })
    }, logs)

    const response = await handleAndWait(handler, commandRequest('login', { userId: OTHER_USER_ID }))

    expect(await response.json()).toEqual({
      response_type: 'ephemeral',
      text: 'Starting Codex device login…'
    })
    const slackResponses = calls
      .filter(call => call.url === RESPONSE_URL)
      .map(call => call.body)
    expect(slackResponses).toHaveLength(2)
    expect(slackResponses).toEqual([
      expect.objectContaining({
        replace_original: true,
        response_type: 'ephemeral',
        text: expect.stringContaining('ABCD-EFGH')
      }),
      expect.objectContaining({
        replace_original: true,
        response_type: 'ephemeral',
        text: expect.stringContaining('is ready in Autorotate')
      })
    ])
    expect(JSON.stringify(slackResponses[1])).not.toContain('ABCD-EFGH')
    expect(JSON.stringify(slackResponses[1])).not.toContain('refresh_token')
    expect(JSON.stringify(logs)).not.toContain('ABCD-EFGH')
    expect(JSON.stringify(logs)).not.toContain('refresh_token')
    expect(JSON.stringify(calls.filter(call => call.url !== RESPONSE_URL))).not.toContain('ABCD-EFGH')
  })

  it('does not expose broker errors or credentials to Slack or logs', async () => {
    const logs: Array<{ data?: unknown; event: string }> = []
    const calls: FetchCall[] = []
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      calls.push({ method: init?.method ?? 'GET', url })
      if (url.includes('/v1/operator/enrollments')) {
        return Response.json(
          {
            error: {
              code: 'provider_login_failed',
              message: 'operator-secret provider response'
            }
          },
          { status: 502 }
        )
      }
      return new Response('', { status: 200 })
    }, logs)

    await handleAndWait(handler, commandRequest('login'))

    const rendered = JSON.stringify(calls) + JSON.stringify(logs)
    expect(rendered).not.toContain('operator-secret')
    expect(rendered).not.toContain('provider response')
    expect(rendered).toContain('provider_login_failed')
  })

  it('accepts an empty successful response when cancelling login', async () => {
    const calls: FetchCall[] = []
    let resolveDeviceCodePosted: (() => void) | undefined
    const deviceCodePosted = new Promise<void>(resolve => {
      resolveDeviceCodePosted = resolve
    })
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      calls.push({ method: init?.method ?? 'GET', url })
      if (url.endsWith('/v1/operator/enrollments')) {
        return Response.json(startEnrollmentResponse(`codex-${USER_ID.toLowerCase()}`), {
          status: 201
        })
      }
      if (init?.method === 'DELETE') return new Response(null, { status: 204 })
      if (url === RESPONSE_URL) {
        resolveDeviceCodePosted?.()
        return new Response('', { status: 200 })
      }
      return Response.json(enrollmentStatusResponse('pending'))
    })
    const pending: Promise<unknown>[] = []

    await handler.handle(commandRequest('login'), promise => pending.push(promise))
    await deviceCodePosted
    const response = await handler.handle(
      commandRequest('login cancel'),
      promise => pending.push(promise)
    )
    expect(response).not.toBeNull()
    await Promise.all(pending)

    expect(calls).toContainEqual({
      method: 'DELETE',
      url: 'https://autorotate.example.test/broker/v1/operator/enrollments/enr_abcdefgh'
    })
  })

  it('renders plain per-account usability, rate limits, resets, and actionable ordering', async () => {
    const calls: FetchCall[] = []
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      calls.push({
        body: init?.body ? JSON.parse(String(init.body)) : undefined,
        method: init?.method ?? 'GET',
        url
      })
      if (url.endsWith('/v1/operator/accounts')) {
        expect(new Headers(init?.headers).get('authorization')).toBe('Bearer operator-secret')
        return Response.json({
          accounts: [
            operatorAccount({
              id: 'must-not-render',
              label: 'broken-primary',
              email: 'broken@example.test',
              status: 'dead',
              login_required: true,
              availability: 'login_required',
              unusable_reason: 'refresh_token_revoked',
              owner: 'must-not-render',
              auth_json: { refresh_token: 'must-not-render' }
            }),
            operatorAccount({
              label: 'legacy-drain',
              email: 'legacy-drain@example.test',
              status: 'disabled',
              login_required: true,
              availability: 'login_required',
              unusable_reason: 'reconciliation_required',
              reconciliation_required: false
            }),
            operatorAccount({
              label: 'reconcile-primary',
              email: 'reconcile@example.test',
              status: 'disabled',
              reconciliation_required: true,
              availability: 'reconciliation_required',
              unusable_reason: 'reconciliation_required'
            }),
            operatorAccount({
              label: 'rate-limited',
              email: 'limited@example.test',
              limited_until: '2026-07-29T22:00:00Z',
              availability: 'rate_limited',
              limits_observed_at: '2026-07-29T20:15:00Z',
              primary: {
                used_percent: 100,
                resets_at: '2026-07-29T22:00:00Z',
                window_minutes: 300
              },
              secondary: {
                used_percent: 99.99,
                resets_at: '2026-08-06T01:42:00Z',
                window_minutes: 10_080
              },
              next_available_at: '2026-07-29T22:00:00Z'
            }),
            operatorAccount({
              label: 'in-use',
              email: 'writer@example.test',
              active_writer: true,
              availability: 'in_use',
              next_available_at: '2026-07-29T21:00:00Z'
            }),
            operatorAccount({
              label: 'ready',
              email: 'available@example.test',
              limits_observed_at: '2026-07-29T20:30:00Z'
            }),
            operatorAccount({
              label: 'disabled',
              email: 'disabled@example.test',
              status: 'disabled',
              availability: 'disabled'
            })
          ]
        })
      }
      return new Response('', { status: 200 })
    })

    const response = await handleAndWait(handler, commandRequest('status'))

    const slackBody = calls.find(call => call.url === RESPONSE_URL)?.body
    const slackText = (slackBody as { text: string }).text
    expect(await response.json()).toEqual({
      response_type: 'ephemeral',
      text: 'Checking Codex account status…'
    })
    expect(slackBody).toMatchObject({
      replace_original: true,
      response_type: 'ephemeral'
    })
    expect(slackText).toContain('Codex accounts: 2 usable / 7')
    expect(slackText).toContain('reason: refresh token revoked')
    expect(slackText).toContain('reason: reconciliation required')
    expect(slackText).toContain('legacy-drain@example.test [legacy-drain]')
    expect(slackText).toContain('reason: out of rate limits')
    expect(slackText).toContain('5h: 100% used; resets 2026-07-29 22:00 UTC')
    expect(slackText).toContain('weekly: 99.99% used; resets 2026-08-06 01:42 UTC')
    expect(slackText).not.toContain('weekly: 100% used')
    expect(slackText).toContain('observed: 2026-07-29 20:15 UTC')
    expect(slackText).toContain('5h: unknown')
    expect(slackText).toContain('weekly: unknown')
    expect(slackText).toContain('observed: unknown')
    const writerBlock = slackText.slice(
      slackText.indexOf('writer@example.test'),
      slackText.indexOf('available@example.test')
    )
    expect(writerBlock).toContain('state: usable')
    expect(writerBlock).toContain('reason: available')
    expect(writerBlock).toContain('next available: now')
    expect(writerBlock).not.toContain('in use')
    expect(writerBlock).not.toContain('2026-07-29 21:00 UTC')
    expect(slackText.indexOf('broken@example.test')).toBeLessThan(
      slackText.indexOf('reconcile@example.test')
    )
    expect(slackText.indexOf('reconcile@example.test')).toBeLessThan(
      slackText.indexOf('limited@example.test')
    )
    expect(slackText.indexOf('limited@example.test')).toBeLessThan(
      slackText.indexOf('writer@example.test')
    )
    expect(slackText.indexOf('writer@example.test')).toBeLessThan(
      slackText.indexOf('available@example.test')
    )
    expect(slackText.indexOf('available@example.test')).toBeLessThan(
      slackText.indexOf('disabled@example.test')
    )
    expect(slackText).not.toContain(':red_circle:')
    expect(slackText).not.toContain(':large_')
    expect(slackText).not.toContain('must-not-render')
    expect(slackText).not.toContain('refresh_token')
  })

  it('renders reset-credit counts and every provider expiry detail', async () => {
    const responses: string[] = []
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      if (url.endsWith('/v1/operator/accounts')) {
        return Response.json({
          accounts: [
            operatorAccount({
              label: 'complete-credits',
              email: 'complete@example.test',
              limits_observed_at: '2026-07-30T00:00:00Z',
              reset_credits: {
                available_count: 2,
                credits: [
                  { expires_at: '2026-08-03T05:45:00Z' },
                  { expires_at: null }
                ]
              }
            }),
            operatorAccount({
              label: 'partial-credits',
              email: 'partial@example.test',
              limits_observed_at: '2026-07-30T00:00:00Z',
              reset_credits: {
                available_count: 3,
                credits: [{ expires_at: '2026-08-04T05:45:00Z' }]
              }
            }),
            operatorAccount({
              label: 'zero-credits',
              email: 'zero@example.test',
              limits_observed_at: '2026-07-30T00:00:00Z',
              reset_credits: {
                available_count: 0,
                credits: []
              }
            }),
            operatorAccount({
              label: 'unknown-credits',
              email: 'unknown@example.test'
            }),
            operatorAccount({
              label: 'null-credits',
              email: 'null@example.test',
              reset_credits: null
            })
          ]
        })
      }
      if (url === RESPONSE_URL && init?.body) {
        responses.push((JSON.parse(String(init.body)) as { text: string }).text)
      }
      return new Response('', { status: 200 })
    })

    await handleAndWait(handler, commandRequest('status'))

    const slackText = responses.at(-1) ?? ''
    expect(slackText).toContain('reset credits: 2 available')
    expect(slackText).toContain('reset 1: expires 2026-08-03 05:45 UTC')
    expect(slackText).toContain('reset 2: does not expire')
    expect(slackText).toContain('reset credits: 3 available')
    expect(slackText).toContain('expiry unavailable for 2 credits')
    expect(slackText).toContain('reset credits: 0 available')
    expect(slackText.match(/reset credits: unknown/g)).toHaveLength(2)
  })

  it('rejects malformed reset-credit counts and expiry details', async () => {
    const invalidResetCredits = [
      { available_count: -1, credits: [] },
      { available_count: 0.5, credits: [] },
      { available_count: 0, credits: [{ expires_at: null }] },
      { available_count: 1, credits: [{ expires_at: 'not-a-time' }] }
    ]

    for (const resetCredits of invalidResetCredits) {
      let slackText = ''
      const handler = testHandler(async (input, init) => {
        const url = String(input)
        if (url.endsWith('/v1/operator/accounts')) {
          return Response.json({
            accounts: [
              operatorAccount({
                limits_observed_at: '2026-07-30T00:00:00Z',
                reset_credits: resetCredits
              })
            ]
          })
        }
        if (url === RESPONSE_URL && init?.body) {
          slackText = (JSON.parse(String(init.body)) as { text: string }).text
        }
        return new Response('', { status: 200 })
      })

      await handleAndWait(handler, commandRequest('status'))
      expect(slackText).toBe('Codex account status is temporarily unavailable.')
    }
  })

  it('keeps 5h and weekly quota rows distinct when only one window is known', async () => {
    const responses: string[] = []
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      if (url.endsWith('/v1/operator/accounts')) {
        return Response.json({
          accounts: [
            operatorAccount({
              label: 'weekly-only',
              email: 'a-weekly@example.test',
              limits_observed_at: '2026-07-30T00:00:00Z',
              primary: {
                used_percent: 0,
                resets_at: '2026-08-06T01:42:00Z',
                window_minutes: 10_080
              }
            }),
            operatorAccount({
              label: 'reversed-windows',
              email: 'b-reversed@example.test',
              limits_observed_at: '2026-07-30T00:00:00Z',
              primary: {
                used_percent: 10,
                resets_at: '2026-08-06T01:42:00Z',
                window_minutes: 10_080
              },
              secondary: {
                used_percent: 20,
                resets_at: '2026-07-30T05:00:00Z',
                window_minutes: 300
              }
            }),
            operatorAccount({
              label: 'five-hour-only',
              email: 'z-five-hour@example.test',
              limits_observed_at: '2026-07-30T00:00:00Z',
              secondary: {
                used_percent: 25,
                resets_at: '2026-07-30T05:00:00Z',
                window_minutes: 300
              }
            })
          ]
        })
      }
      if (url === RESPONSE_URL && init?.body) {
        responses.push((JSON.parse(String(init.body)) as { text: string }).text)
      }
      return new Response('', { status: 200 })
    })

    await handleAndWait(handler, commandRequest('status'))

    const slackText = responses.at(-1) ?? ''
    const weeklyOnly = slackText.slice(
      slackText.indexOf('a-weekly@example.test'),
      slackText.indexOf('b-reversed@example.test')
    )
    const reversed = slackText.slice(
      slackText.indexOf('b-reversed@example.test'),
      slackText.indexOf('z-five-hour@example.test')
    )
    const fiveHourOnly = slackText.slice(slackText.indexOf('z-five-hour@example.test'))
    expect(weeklyOnly).toContain('5h: unknown')
    expect(weeklyOnly).toContain('weekly: 0% used; resets 2026-08-06 01:42 UTC')
    expect(weeklyOnly.match(/weekly:/g)).toHaveLength(1)
    expect(reversed.indexOf('5h: 20% used')).toBeLessThan(
      reversed.indexOf('weekly: 10% used')
    )
    expect(fiveHourOnly).toContain('5h: 25% used; resets 2026-07-30 05:00 UTC')
    expect(fiveHourOnly).toContain('weekly: unknown')
    expect(fiveHourOnly.match(/5h:/g)).toHaveLength(1)
  })

  it('rejects malformed or contradictory operator account status fields', async () => {
    const reset = '2026-07-29T22:00:00Z'
    const invalidAccounts = [
      operatorAccount({ availability: 'usable' }),
      operatorAccount({ unusable_reason: 'refresh_token' }),
      operatorAccount({ active_writer: 'false' }),
      operatorAccount({ primary: undefined }),
      operatorAccount({ limits_observed_at: undefined }),
      operatorAccount({ limits_observed_at: 'not-a-time' }),
      operatorAccount({
        primary: {
          used_percent: 12,
          resets_at: reset,
          window_minutes: 300
        }
      }),
      operatorAccount({
        primary: {
          used_percent: 101,
          resets_at: 'not-a-time',
          window_minutes: 300
        }
      }),
      operatorAccount({ status: 'dead' }),
      operatorAccount({ login_required: true }),
      operatorAccount({ reconciliation_required: true }),
      operatorAccount({ active_writer: true }),
      operatorAccount({ limited_until: reset }),
      operatorAccount({ unusable_reason: 'refresh_token_revoked' }),
      operatorAccount({ next_available_at: reset }),
      operatorAccount({
        availability: 'in_use',
        next_available_at: reset
      }),
      operatorAccount({
        availability: 'rate_limited',
        next_available_at: reset
      }),
      operatorAccount({
        availability: 'rate_limited',
        limited_until: reset,
        next_available_at: '2026-07-29T21:00:00Z'
      }),
      operatorAccount({
        availability: 'login_required',
        status: 'dead',
        login_required: true
      }),
      operatorAccount({
        availability: 'reconciliation_required',
        status: 'disabled',
        reconciliation_required: true,
        unusable_reason: 'login_required'
      }),
      operatorAccount({ availability: 'disabled' })
    ]

    for (const account of invalidAccounts) {
      let slackText = ''
      const handler = testHandler(async (input, init) => {
        const url = String(input)
        if (url.endsWith('/v1/operator/accounts')) {
          return Response.json({ accounts: [account] })
        }
        if (url === RESPONSE_URL) {
          slackText = (JSON.parse(String(init?.body)) as { text: string }).text
        }
        return new Response('', { status: 200 })
      })

      await handleAndWait(handler, commandRequest('status'))
      expect(slackText).toBe('Codex account status is temporarily unavailable.')
    }
  })

  it('deterministically truncates status within Slack text limits', async () => {
    const accounts = Array.from({ length: 80 }, (_, index) => {
      const number = String(index).padStart(3, '0')
      return operatorAccount({
        label: `account-${number}-${'l'.repeat(116)}`,
        email: `account-${number}.${'e'.repeat(220)}@example.test`
      })
    })
    let slackText = ''
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      if (url.endsWith('/v1/operator/accounts')) {
        return Response.json({ accounts })
      }
      if (url === RESPONSE_URL) {
        slackText = (JSON.parse(String(init?.body)) as { text: string }).text
      }
      return new Response('', { status: 200 })
    })

    await handleAndWait(handler, commandRequest('status'))

    expect(slackText.length).toBeLessThanOrEqual(35_000)
    expect(slackText).toStartWith('Codex accounts: 80 usable / 80')
    expect(slackText).toContain('account-000')
    expect(slackText).toMatch(/\d+ more accounts not shown\.$/)
    expect(slackText).not.toContain('account-079')
  })

  it('uses targetless provider discovery for canonical login', async () => {
    const calls: FetchCall[] = []
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      const body = init?.body ? JSON.parse(String(init.body)) : undefined
      calls.push({ body, method: init?.method ?? 'GET', url })
      if (url.endsWith('/v1/operator/enrollments') && init?.method === 'POST') {
        expect(body).toEqual({
          action: 'relogin',
          owner: `slack:${TEAM_ID}:${USER_ID}`
        })
        const start = startEnrollmentResponse('livermore-ci-legacy', {
          action: 'relogin'
        })
        delete start.account_label
        return Response.json(start, { status: 201 })
      }
      if (url.endsWith('/v1/operator/enrollments/enr_abcdefgh')) {
        return Response.json(enrollmentStatusResponse('completed', {
          accountLabel: 'livermore-ci-legacy'
        }))
      }
      return new Response('', { status: 200 })
    })

    await handleAndWait(handler, commandRequest('login'))

    const slackBodies = calls
      .filter(call => call.url === RESPONSE_URL)
      .map(call => JSON.stringify(call.body))
    expect(slackBodies[0]).toContain('ABCD-EFGH')
    expect(slackBodies[1]).toContain('is ready in Autorotate')
    expect(calls.some(call => call.url.endsWith('/v1/operator/accounts'))).toBe(false)
  })

  it('continues an existing owner login regardless of its compatibility action', async () => {
    const calls: FetchCall[] = []
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      calls.push({
        body: init?.body ? JSON.parse(String(init.body)) : undefined,
        method: init?.method ?? 'GET',
        url
      })
      if (url.endsWith('/v1/operator/enrollments') && init?.method === 'POST') {
        return Response.json(startEnrollmentResponse('pending-account', {
          action: 'add'
        }))
      }
      if (url.endsWith('/v1/operator/enrollments/enr_abcdefgh')) {
        return Response.json(enrollmentStatusResponse('completed'))
      }
      return new Response('', { status: 200 })
    })

    await handleAndWait(handler, commandRequest('login'))

    expect(calls.some(call => call.method === 'DELETE')).toBe(false)
    expect(JSON.stringify(calls.filter(call => call.url === RESPONSE_URL))).toContain(
      'is ready in Autorotate'
    )
  })

  it('maps add and relogin aliases to the same targetless login request', async () => {
    for (const alias of ['add', 'relogin']) {
      let enrollmentBody: unknown
      const handler = testHandler(async (input, init) => {
        const url = String(input)
        if (url.endsWith('/v1/operator/enrollments') && init?.method === 'POST') {
          enrollmentBody = JSON.parse(String(init.body))
          return Response.json(startEnrollmentResponse('team-codex'), { status: 201 })
        }
        if (url.endsWith('/v1/operator/enrollments/enr_abcdefgh')) {
          return Response.json(enrollmentStatusResponse('completed'))
        }
        return new Response('', { status: 200 })
      })

      await handleAndWait(handler, commandRequest(alias))

      expect(enrollmentBody).toEqual({
        action: 'relogin',
        owner: `slack:${TEAM_ID}:${USER_ID}`
      })
    }
  })

  it('maps accounts to the same status view', async () => {
    const rendered: string[] = []
    for (const command of ['status', 'accounts']) {
      const handler = testHandler(async (input, init) => {
        const url = String(input)
        if (url.endsWith('/v1/operator/accounts')) {
          return Response.json({ accounts: [operatorAccount()] })
        }
        if (url === RESPONSE_URL) {
          rendered.push((JSON.parse(String(init?.body)) as { text: string }).text)
        }
        return new Response('', { status: 200 })
      })
      await handleAndWait(handler, commandRequest(command))
    }

    expect(rendered).toHaveLength(2)
    expect(rendered[0]).toBe(rendered[1])
  })

  it('rejects labels, emails, and targeted relogin syntax', async () => {
    for (const command of [
      'login label',
      'login label person@example.test',
      'login relogin legacy-primary',
      'add label',
      'relogin legacy-primary'
    ]) {
      const handler = testHandler(async () => {
        throw new Error('must not fetch')
      })
      const response = await handleAndWait(handler, commandRequest(command))
      expect(await response.json()).toMatchObject({
        text: expect.stringContaining('Autorotate commands')
      })
    }
  })

  it('installs the local starting lease before creating an enrollment', async () => {
    let createCount = 0
    let releaseCreate: ((response: Response) => void) | undefined
    const createResponse = new Promise<Response>(resolve => {
      releaseCreate = resolve
    })
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      if (url.endsWith('/v1/operator/enrollments') && init?.method === 'POST') {
        createCount += 1
        return createResponse
      }
      if (url.endsWith('/v1/operator/enrollments/enr_abcdefgh')) {
        return Response.json(enrollmentStatusResponse('completed', { accountLabel: 'first' }))
      }
      return new Response('', { status: 200 })
    })
    const pending: Promise<unknown>[] = []

    const first = await handler.handle(
      commandRequest('login'),
      promise => pending.push(promise)
    )
    const second = await handler.handle(
      commandRequest('login'),
      promise => pending.push(promise)
    )

    expect(await first?.json()).toMatchObject({ text: 'Starting Codex device login…' })
    expect(await second?.json()).toMatchObject({ text: expect.stringContaining('already') })
    expect(createCount).toBe(1)
    releaseCreate?.(Response.json(startEnrollmentResponse('first'), { status: 201 }))
    await Promise.all(pending)
  })

  it('lets cancellation win over a concurrent completed poll', async () => {
    const slackBodies: string[] = []
    let markDevicePosted: (() => void) | undefined
    const devicePosted = new Promise<void>(resolve => {
      markDevicePosted = resolve
    })
    let markPollStarted: (() => void) | undefined
    const pollStarted = new Promise<void>(resolve => {
      markPollStarted = resolve
    })
    let finishPoll: ((response: Response) => void) | undefined
    const pollResponse = new Promise<Response>(resolve => {
      finishPoll = resolve
    })
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      if (url.endsWith('/v1/operator/enrollments') && init?.method === 'POST') {
        return Response.json(startEnrollmentResponse(`codex-${USER_ID.toLowerCase()}`), {
          status: 201
        })
      }
      if (
        url.endsWith('/v1/operator/enrollments/enr_abcdefgh')
        && init?.method === 'GET'
      ) {
        markPollStarted?.()
        return pollResponse
      }
      if (init?.method === 'DELETE') return new Response(null, { status: 204 })
      if (url === RESPONSE_URL) {
        slackBodies.push(String(init?.body))
        markDevicePosted?.()
        return new Response('', { status: 200 })
      }
      return Response.json({})
    })
    const pending: Promise<unknown>[] = []

    await handler.handle(commandRequest('login'), promise => pending.push(promise))
    await devicePosted
    await pollStarted
    await handler.handle(commandRequest('login cancel'), promise => pending.push(promise))
    finishPoll?.(Response.json(enrollmentStatusResponse('completed', {
      accountLabel: `codex-${USER_ID.toLowerCase()}`
    })))
    await Promise.all(pending)

    expect(slackBodies.some(body => body.includes('is ready in Autorotate'))).toBe(false)
    expect(slackBodies.some(body => body.includes('was cancelled'))).toBe(true)
  })

  it('cleans up upstream and local state when ephemeral code delivery fails', async () => {
    let createCount = 0
    let cancelCount = 0
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      if (url.endsWith('/v1/operator/enrollments') && init?.method === 'POST') {
        createCount += 1
        return Response.json(startEnrollmentResponse('team-codex', {
          enrollmentId: `enr_abcdefg${createCount}`
        }), { status: 201 })
      }
      if (init?.method === 'DELETE') {
        cancelCount += 1
        return new Response(null, { status: 204 })
      }
      if (url === RESPONSE_URL) return new Response('', { status: 500 })
      return Response.json({})
    })

    await handleAndWait(handler, commandRequest('login'))
    const second = await handleAndWait(handler, commandRequest('login'))

    expect(await second.json()).toMatchObject({ text: 'Starting Codex device login…' })
    expect(createCount).toBe(2)
    expect(cancelCount).toBe(2)
  })

  it('continues polling when a repeated start is already importing', async () => {
    const calls: FetchCall[] = []
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      calls.push({
        body: init?.body ? JSON.parse(String(init.body)) : undefined,
        method: init?.method ?? 'GET',
        url
      })
      if (url.endsWith('/v1/operator/enrollments') && init?.method === 'POST') {
        return Response.json(startEnrollmentResponse('team-codex', {
          status: 'importing'
        }))
      }
      if (url.endsWith('/v1/operator/enrollments/enr_abcdefgh')) {
        return Response.json(enrollmentStatusResponse('completed'))
      }
      return new Response('', { status: 200 })
    })

    await handleAndWait(handler, commandRequest('login'))

    const slackBodies = calls
      .filter(call => call.url === RESPONSE_URL)
      .map(call => JSON.stringify(call.body))
    expect(slackBodies).toHaveLength(2)
    expect(slackBodies[0]).toContain('authorized and importing')
    expect(slackBodies[0]).not.toContain('ABCD-EFGH')
    expect(slackBodies[1]).toContain('is ready in Autorotate')
  })

  it('rejects a pending start response without both device credentials', async () => {
    const calls: FetchCall[] = []
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      calls.push({
        body: init?.body ? JSON.parse(String(init.body)) : undefined,
        method: init?.method ?? 'GET',
        url
      })
      if (url.endsWith('/v1/operator/enrollments') && init?.method === 'POST') {
        const response = startEnrollmentResponse('team-codex')
        delete response.user_code
        return Response.json(response)
      }
      return new Response('', { status: 200 })
    })

    await handleAndWait(handler, commandRequest('login'))

    const slackBodies = calls
      .filter(call => call.url === RESPONSE_URL)
      .map(call => JSON.stringify(call.body))
    expect(slackBodies).toHaveLength(1)
    expect(slackBodies[0]).toContain('could not be started')
    expect(slackBodies[0]).not.toContain('auth.openai.com')
  })

  it('recovers active login by exact Slack owner after a process restart', async () => {
    const calls: FetchCall[] = []
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      calls.push({ method: init?.method ?? 'GET', url })
      if (url.includes('/v1/operator/enrollments?owner=')) {
        return Response.json(startEnrollmentResponse('team-codex'))
      }
      if (url.endsWith('/v1/operator/enrollments/enr_abcdefgh')) {
        return Response.json(enrollmentStatusResponse('completed'))
      }
      return new Response('', { status: 200 })
    })

    await handleAndWait(handler, commandRequest('login status'))

    expect(calls).toContainEqual({
      method: 'GET',
      url: `https://autorotate.example.test/broker/v1/operator/enrollments?owner=${encodeURIComponent(`slack:${TEAM_ID}:${USER_ID}`)}`
    })
    expect(calls.filter(call => call.url === RESPONSE_URL)).toHaveLength(2)
  })

  it('recovers an importing login without expecting or exposing a device code', async () => {
    const calls: FetchCall[] = []
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      calls.push({
        body: init?.body ? JSON.parse(String(init.body)) : undefined,
        method: init?.method ?? 'GET',
        url
      })
      if (url.includes('/v1/operator/enrollments?owner=')) {
        const response = startEnrollmentResponse('team-codex', {
          status: 'importing'
        })
        response.expires_at = new Date(Date.now() - 1_000).toISOString()
        return Response.json(response)
      }
      if (url.endsWith('/v1/operator/enrollments/enr_abcdefgh')) {
        return Response.json(enrollmentStatusResponse('completed'))
      }
      return new Response('', { status: 200 })
    })

    await handleAndWait(handler, commandRequest('login status'))

    const slackBodies = calls
      .filter(call => call.url === RESPONSE_URL)
      .map(call => JSON.stringify(call.body))
    expect(slackBodies).toHaveLength(2)
    expect(slackBodies[0]).toContain('authorized and importing')
    expect(slackBodies[0]).toContain('team-codex')
    expect(slackBodies[0]).not.toContain('ABCD-EFGH')
    expect(slackBodies[0]).not.toContain('auth.openai.com')
    expect(slackBodies[1]).toContain('is ready in Autorotate')
  })

  it('resumes monitoring when an importing recovery can no longer be cancelled', async () => {
    const calls: FetchCall[] = []
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      calls.push({
        body: init?.body ? JSON.parse(String(init.body)) : undefined,
        method: init?.method ?? 'GET',
        url
      })
      if (url.includes('/v1/operator/enrollments?owner=')) {
        return Response.json(startEnrollmentResponse('team-codex', {
          status: 'importing'
        }))
      }
      if (
        url.endsWith('/v1/operator/enrollments/enr_abcdefgh')
        && init?.method === 'DELETE'
      ) {
        return Response.json(enrollmentStatusResponse('importing'))
      }
      if (url.endsWith('/v1/operator/enrollments/enr_abcdefgh')) {
        return Response.json(enrollmentStatusResponse('completed'))
      }
      return new Response('', { status: 200 })
    })

    await handleAndWait(handler, commandRequest('login cancel'))

    const slackBodies = calls
      .filter(call => call.url === RESPONSE_URL)
      .map(call => JSON.stringify(call.body))
    expect(slackBodies).toHaveLength(2)
    expect(slackBodies[0]).toContain('can no longer be cancelled')
    expect(slackBodies[1]).toContain('is ready in Autorotate')
  })
})
