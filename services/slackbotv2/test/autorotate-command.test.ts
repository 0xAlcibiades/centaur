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
const RESPONSE_URL = 'https://hooks.slack.test/commands/response-secret'

type FetchCall = {
  body?: unknown
  method: string
  url: string
}

function startEnrollmentResponse(
  accountLabel: string,
  {
    action = 'add',
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
    enrollmentId = 'enr_abcdefgh'
  }: {
    accountLabel?: string
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
    error_code: status === 'failed' ? 'provider_login_failed' : null
  }
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
    channelId = 'D123456789',
    signatureValid = true,
    teamId = TEAM_ID,
    userId = USER_ID
  }: {
    channelId?: string
    signatureValid?: boolean
    teamId?: string
    userId?: string
  } = {}
): Request {
  const body = new URLSearchParams({
    channel_id: channelId,
    command: '/autorotate',
    response_url: RESPONSE_URL,
    team_id: teamId,
    text,
    user_id: userId
  }).toString()
  const timestamp = String(Math.floor(Date.now() / 1000))
  const signature = createHmac('sha256', SIGNING_SECRET)
    .update(`v0:${timestamp}:${body}`)
    .digest('hex')
  return new Request('https://bot.example.test/api/slack/commands', {
    method: 'POST',
    headers: {
      'content-type': 'application/x-www-form-urlencoded',
      'x-slack-request-timestamp': timestamp,
      'x-slack-signature': `v0=${signatureValid ? signature : '0'.repeat(64)}`
    },
    body
  })
}

function testHandler(
  fetchFn: SlackbotV2Fetch,
  logs: Array<{ data?: unknown; event: string }> = []
) {
  return createAutorotateSlackCommandHandler({
    brokerUrl: 'https://autorotate.example.test/broker/',
    fetch: fetchFn,
    logger: logger(logs),
    observerToken: 'observer-secret',
    operatorSlackTeamIds: [TEAM_ID],
    operatorSlackUserIds: [USER_ID],
    operatorToken: 'operator-secret',
    pollIntervalMs: 1,
    requestTimeoutMs: 1_000,
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
      if (url.includes('/v1/status')) {
        return Response.json({
          total: 2,
          healthy: 2,
          available: 2,
          limited: 0,
          login_required: 0,
          disabled: 0,
          leased: 0,
          removed: 0,
          pending_enrollments: 0
        })
      }
      return new Response('', { status: 200 })
    }
    const bot = createSlackbotV2({
      apiUrl: 'https://centaur-api.example.test',
      autorotateObserverToken: 'observer-secret',
      autorotateOperatorToken: 'operator-secret',
      autorotateSlackResponseUrlHosts: ['hooks.slack.test'],
      autorotateSlackTeamIds: [TEAM_ID],
      autorotateSlackUserIds: [USER_ID],
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
    expect(calls.some(url => url.includes('/v1/status'))).toBe(true)
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

  it('requires both the configured Slack workspace and member', async () => {
    const handler = testHandler(async () => {
      throw new Error('must not fetch')
    })

    const response = await handleAndWait(
      handler,
      commandRequest('status', { userId: 'U999999999' })
    )

    expect(response.status).toBe(200)
    expect(await response.json()).toEqual({
      response_type: 'ephemeral',
      text: 'You are not authorized to operate the Codex account pool.'
    })
  })

  it('returns aggregate status through the observer token', async () => {
    const calls: FetchCall[] = []
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      const body = init?.body ? JSON.parse(String(init.body)) : undefined
      calls.push({ body, method: init?.method ?? 'GET', url })
      if (url.includes('/v1/status')) {
        expect(new Headers(init?.headers).get('authorization')).toBe('Bearer observer-secret')
        return Response.json({
          generated_at: '2026-07-29T12:00:00Z',
          total: 5,
          healthy: 4,
          available: 3,
          limited: 1,
          login_required: 1,
          disabled: 0,
          leased: 1,
          removed: 0,
          next_available_at: null,
          pending_enrollments: 0,
          accounts: [{ label: 'must-not-render' }]
        })
      }
      return new Response('', { status: 200 })
    })

    const response = await handleAndWait(handler, commandRequest('status'))

    expect(response.status).toBe(200)
    const slackCall = calls.find(call => call.url === RESPONSE_URL)
    expect(slackCall).toBeDefined()
    expect(JSON.stringify(slackCall?.body)).toContain('Available: 3 / 5')
    expect(JSON.stringify(slackCall?.body)).not.toContain('must-not-render')
  })

  it('refuses to start login outside a DM', async () => {
    const handler = testHandler(async () => {
      throw new Error('must not fetch')
    })

    const response = await handleAndWait(
      handler,
      commandRequest('login', { channelId: 'C123456789' })
    )

    expect(await response.json()).toEqual({
      response_type: 'ephemeral',
      text: 'For account security, run this command in a DM with me.'
    })
  })

  it('sends a device code only to the private response URL and replaces it on completion', async () => {
    const calls: FetchCall[] = []
    const logs: Array<{ data?: unknown; event: string }> = []
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      const body = init?.body ? JSON.parse(String(init.body)) : undefined
      calls.push({ body, method: init?.method ?? 'GET', url })
      if (url.endsWith('/v1/operator/enrollments')) {
        expect(new Headers(init?.headers).get('authorization')).toBe('Bearer operator-secret')
        expect(body).toEqual({
          action: 'add',
          expected_email: 'person@example.test',
          label: 'team-codex',
          owner: `slack:${TEAM_ID}:${USER_ID}`
        })
        return Response.json({
          ...startEnrollmentResponse('team-codex'),
          auth_json: { refresh_token: 'must-not-render' }
        }, { status: 201 })
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

    const response = await handleAndWait(
      handler,
      commandRequest('login team-codex person@example.test')
    )

    expect(await response.json()).toEqual({
      response_type: 'ephemeral',
      text: 'Starting a private Codex device login…'
    })
    const slackBodies = calls
      .filter(call => call.url === RESPONSE_URL)
      .map(call => JSON.stringify(call.body))
    expect(slackBodies).toHaveLength(2)
    expect(slackBodies[0]).toContain('ABCD-EFGH')
    expect(slackBodies[0]).toContain('response_type')
    expect(slackBodies[1]).not.toContain('ABCD-EFGH')
    expect(slackBodies[1]).not.toContain('refresh_token')
    expect(slackBodies[1]).toContain('was added to Autorotate')
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

  it('shows operator-only account emails and status in a private response', async () => {
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
            {
              label: 'primary',
              email: 'person@example.test',
              status: 'enabled',
              limited_until: null,
              login_required: true,
              auth_json: { refresh_token: 'must-not-render' }
            }
          ]
        })
      }
      return new Response('', { status: 200 })
    })

    await handleAndWait(handler, commandRequest('accounts'))

    const slackBody = calls.find(call => call.url === RESPONSE_URL)?.body
    expect(JSON.stringify(slackBody)).toContain('person@example.test')
    expect(JSON.stringify(slackBody)).toContain('login required')
    expect(JSON.stringify(slackBody)).not.toContain('refresh_token')
  })

  it('uses the safe account list to start a relogin by label and expected email', async () => {
    const calls: FetchCall[] = []
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      const body = init?.body ? JSON.parse(String(init.body)) : undefined
      calls.push({ body, method: init?.method ?? 'GET', url })
      if (url.endsWith('/v1/operator/accounts')) {
        return Response.json({
          accounts: [
            {
              label: 'livermore-ci-legacy',
              email: 'person@example.test',
              status: 'dead',
              limited_until: null,
              login_required: true
            }
          ]
        })
      }
      if (url.endsWith('/v1/operator/enrollments') && init?.method === 'POST') {
        expect(body).toEqual({
          action: 'relogin',
          account: 'livermore-ci-legacy',
          expected_email: 'person@example.test',
          owner: `slack:${TEAM_ID}:${USER_ID}`
        })
        return Response.json(startEnrollmentResponse('livermore-ci-legacy', {
          action: 'relogin'
        }), { status: 201 })
      }
      if (url.endsWith('/v1/operator/enrollments/enr_abcdefgh')) {
        return Response.json(enrollmentStatusResponse('completed', {
          accountLabel: 'livermore-ci-legacy'
        }))
      }
      return new Response('', { status: 200 })
    })

    await handleAndWait(handler, commandRequest('login relogin livermore-ci-legacy'))

    const slackBodies = calls
      .filter(call => call.url === RESPONSE_URL)
      .map(call => JSON.stringify(call.body))
    expect(slackBodies[0]).toContain('Reauthenticating')
    expect(slackBodies[0]).toContain('person@example.test')
    expect(slackBodies[1]).toContain('was added to Autorotate')
  })

  it('accepts a JSON-quoted relogin label while preserving the exact account name', async () => {
    let reloginBody: unknown
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      if (url.endsWith('/v1/operator/accounts')) {
        return Response.json({
          accounts: [
            {
              label: 'legacy "primary" \\ ci',
              email: 'person@example.test',
              status: 'dead',
              limited_until: null,
              login_required: true
            }
          ]
        })
      }
      if (url.endsWith('/v1/operator/enrollments') && init?.method === 'POST') {
        reloginBody = JSON.parse(String(init.body))
        return Response.json(startEnrollmentResponse('legacy "primary" \\ ci', {
          action: 'relogin'
        }), { status: 201 })
      }
      if (url.endsWith('/v1/operator/enrollments/enr_abcdefgh')) {
        return Response.json(enrollmentStatusResponse('completed', {
          accountLabel: 'legacy "primary" \\ ci'
        }))
      }
      return new Response('', { status: 200 })
    })

    await handleAndWait(
      handler,
      commandRequest('login relogin "legacy \\"primary\\" \\\\ ci"')
    )

    expect(reloginBody).toMatchObject({
      account: 'legacy "primary" \\ ci',
      action: 'relogin'
    })
  })

  it('rejects trailing junk after a JSON-quoted relogin label', async () => {
    const calls: FetchCall[] = []
    const handler = testHandler(async (input, init) => {
      calls.push({
        body: init?.body ? JSON.parse(String(init.body)) : undefined,
        method: init?.method ?? 'GET',
        url: String(input)
      })
      return new Response('', { status: 200 })
    })

    const response = await handleAndWait(
      handler,
      commandRequest('login relogin "legacy primary" trailing junk')
    )

    expect(calls).toEqual([])
    expect(await response.json()).toMatchObject({
      text: expect.stringContaining('*Autorotate commands*')
    })
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
      commandRequest('login first'),
      promise => pending.push(promise)
    )
    const second = await handler.handle(
      commandRequest('login second'),
      promise => pending.push(promise)
    )

    expect(await first?.json()).toMatchObject({ text: 'Starting a private Codex device login…' })
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

    expect(slackBodies.some(body => body.includes('was added to Autorotate'))).toBe(false)
    expect(slackBodies.some(body => body.includes('was cancelled'))).toBe(true)
  })

  it('cleans up upstream and local state when private code delivery fails', async () => {
    let createCount = 0
    let cancelCount = 0
    const handler = testHandler(async (input, init) => {
      const url = String(input)
      if (url.endsWith('/v1/operator/enrollments') && init?.method === 'POST') {
        createCount += 1
        const body = JSON.parse(String(init.body)) as { label: string }
        return Response.json(startEnrollmentResponse(body.label, {
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

    await handleAndWait(handler, commandRequest('login first'))
    const second = await handleAndWait(handler, commandRequest('login second'))

    expect(await second.json()).toMatchObject({ text: 'Starting a private Codex device login…' })
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

    await handleAndWait(handler, commandRequest('login team-codex'))

    const slackBodies = calls
      .filter(call => call.url === RESPONSE_URL)
      .map(call => JSON.stringify(call.body))
    expect(slackBodies).toHaveLength(2)
    expect(slackBodies[0]).toContain('authorized and importing')
    expect(slackBodies[0]).not.toContain('ABCD-EFGH')
    expect(slackBodies[1]).toContain('was added to Autorotate')
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

    await handleAndWait(handler, commandRequest('login team-codex'))

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
    expect(slackBodies[1]).toContain('was added to Autorotate')
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
    expect(slackBodies[1]).toContain('was added to Autorotate')
  })
})
