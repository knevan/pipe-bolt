import type { Page, Route } from '@playwright/test'

import type {
  PipeBoltApiDtoProjectConfigDocumentV1,
  PipeBoltApiDtoRuntimeStatusResponse,
} from '../../src/api/generated/types.gen'

export const E2E_PROJECT_ID = 'e2e-project'
export const E2E_TOKEN = '0123456789abcdef0123456789abcdef'

export interface MockApiState {
  config: PipeBoltApiDtoProjectConfigDocumentV1
  configVersion: number
  failureResolved: boolean
  lastCommandRequest?: unknown
  lastConfigWrite?: unknown
  lastResolutionRequest?: unknown
  unexpectedRequests: string[]
}

interface MockApiOptions {
  runtimeDelayMs?: number
}

function configFixture(): PipeBoltApiDtoProjectConfigDocumentV1 {
  return {
    brokers: [
      {
        clean_session: true,
        client_id: 'pipe-bolt-e2e',
        credentials: null,
        enabled: true,
        host: 'mqtt.fixture.test',
        id: 'broker-main',
        keep_alive: 30,
        name: 'Primary Broker',
        port: 1883,
        reconnect: { max_delay: 30_000, min_delay: 500 },
        tls: 'disabled',
      },
    ],
    command_templates: [
      {
        broker_id: 'broker-main',
        enabled: true,
        id: 'command-reboot',
        name: 'Reboot device',
        payload_template: { action: 'reboot' },
        qos: 'at_least_once',
        retain: false,
        topic_template: 'devices/{device_id}/commands',
      },
    ],
    description: 'Playwright fixture',
    enabled: true,
    name: 'E2E Project',
    project_id: E2E_PROJECT_ID,
    routes: [
      {
        backpressure: 'reject',
        broker_id: 'broker-main',
        codec: 'json',
        device_id: { type: 'none' },
        enabled: true,
        event_type: 'telemetry',
        id: 'route-main',
        name: 'Telemetry',
        qos: 'at_least_once',
        schema_mapping_id: null,
        topic_filter: 'devices/+/telemetry',
      },
    ],
    rules: [],
    schema_mappings: [],
    sinks: [],
    tenant_id: null,
  }
}

function runtimeFixture(version: number): PipeBoltApiDtoRuntimeStatusResponse {
  return {
    active_version: version,
    counters: {
      forwarder: {
        accepted_total: 0,
        backpressure_total: 0,
        delivered_total: 0,
        failed_total: 0,
        outcome_dropped_total: 0,
        rejected_total: 0,
        response_too_large_total: 0,
        timed_out_total: 0,
      },
      persistence_writer: null,
      pipeline: {
        action_intent_total: 0,
        delivery_outcome_persist_failed_total: 0,
        dispatch_failed_total: 0,
        forward_outcome_total: 0,
        matched_rule_total: 0,
        normalized_total: 0,
        realtime_event_no_receiver_total: 0,
        realtime_event_published_total: 0,
      },
    },
    last_reload_at: null,
    last_reload_error: null,
    project_id: E2E_PROJECT_ID,
    started_at: '2026-07-20T09:00:00Z',
    state: 'running',
  }
}

async function fulfillJson(route: Route, body: unknown, status = 200): Promise<void> {
  await route.fulfill({
    body: JSON.stringify(body),
    contentType: 'application/json',
    status,
  })
}

function failureFixture() {
  return {
    component: 'forwarder',
    details: { attempt: 1 },
    event_id: 'event-1',
    failure_id: 'failure-1',
    failure_kind: 'delivery_timeout',
    message: 'Webhook delivery timed out',
    occurred_at: '2026-07-20T09:30:00Z',
    project_id: E2E_PROJECT_ID,
    resolution: null,
    resolved_at: null,
    severity: 'error',
    sink_id: 'sink-webhook',
  }
}

export async function installApiMocks(
  page: Page,
  options: MockApiOptions = {},
): Promise<MockApiState> {
  const state: MockApiState = {
    config: configFixture(),
    configVersion: 7,
    failureResolved: false,
    unexpectedRequests: [],
  }

  await page.route('**/projects/**', async (route, request) => {
    if (request.resourceType() === 'document') {
      await route.continue()
      return
    }
    const url = new URL(request.url())
    const method = request.method()
    const requestKey = `${method} ${url.pathname}`
    if ((await request.headerValue('authorization')) !== `Bearer ${E2E_TOKEN}`) {
      state.unexpectedRequests.push(`${requestKey} missing bearer token`)
      await fulfillJson(
        route,
        { error: { code: 'unauthorized', message: 'Missing bearer token.' } },
        401,
      )
      return
    }

    if (method === 'GET' && url.pathname === `/projects/${E2E_PROJECT_ID}/runtime/status`) {
      if (options.runtimeDelayMs) {
        await new Promise((resolve) => setTimeout(resolve, options.runtimeDelayMs))
      }
      await fulfillJson(route, runtimeFixture(state.configVersion))
      return
    }

    if (url.pathname === `/projects/${E2E_PROJECT_ID}/config`) {
      if (method === 'GET') {
        await fulfillJson(route, {
          config: state.config,
          schema_version: 1,
          version: state.configVersion,
        })
        return
      }
      if (method === 'PUT') {
        const body = request.postDataJSON() as {
          config: PipeBoltApiDtoProjectConfigDocumentV1
          expected_version: number
          reason?: string
        }
        state.lastConfigWrite = body
        state.config = structuredClone(body.config)
        state.configVersion += 1
        await fulfillJson(route, {
          config_hash: `sha256:e2e-v${state.configVersion}`,
          project_id: E2E_PROJECT_ID,
          reload_required: true,
          revision_id: `revision-${state.configVersion}`,
          version: state.configVersion,
        })
        return
      }
    }

    if (
      method === 'POST' &&
      url.pathname === `/projects/${E2E_PROJECT_ID}/commands/command-reboot/execute`
    ) {
      state.lastCommandRequest = request.postDataJSON()
      await fulfillJson(
        route,
        {
          audit_event_id: 'audit-command-1',
          broker_id: 'broker-main',
          command_execution_id: 'command-exec-1',
          command_template_id: 'command-reboot',
          payload_size_bytes: 19,
          project_id: E2E_PROJECT_ID,
          qos: 'at_least_once',
          queued_at: '2026-07-20T10:00:00Z',
          retain: false,
          status: 'queued',
          topic: 'devices/device-1/commands',
        },
        202,
      )
      return
    }

    if (method === 'GET' && url.pathname === `/projects/${E2E_PROJECT_ID}/audit-events`) {
      await fulfillJson(route, { items: [], limit: 100, next_before: null })
      return
    }

    if (method === 'GET' && url.pathname === `/projects/${E2E_PROJECT_ID}/realtime/sse`) {
      const ready = {
        filter: {
          device_id: null,
          event_type: null,
          route_id: null,
          topic: null,
          topic_prefix: null,
        },
        transport: 'sse',
        type: 'ready',
      }
      const event = {
        data: {
          broker_id: 'broker-main',
          correlation_id: 'correlation-event-1',
          device_id: 'device-1',
          event_type: 'temperature_update',
          fields: { temperature: 21 },
          id: 'event-1',
          metadata: {},
          normalization_errors: [],
          payload: { type: 'json', value: { temperature: 21 } },
          payload_size_bytes: 18,
          project_id: E2E_PROJECT_ID,
          raw: null,
          received_at: '2026-07-20T10:00:00Z',
          route_id: 'route-main',
          schema_mapping_id: null,
          topic: 'devices/device-1/telemetry',
        },
        type: 'event',
      }
      await route.fulfill({
        body: `event: ready\ndata: ${JSON.stringify(ready)}\n\nevent: event\ndata: ${JSON.stringify(event)}\n\n`,
        contentType: 'text/event-stream',
        headers: { 'cache-control': 'no-cache' },
        status: 200,
      })
      return
    }

    if (method === 'GET' && url.pathname === `/projects/${E2E_PROJECT_ID}/failures`) {
      await fulfillJson(route, {
        items: state.failureResolved ? [] : [failureFixture()],
        limit: 100,
        next_before: null,
      })
      return
    }

    if (
      method === 'POST' &&
      url.pathname === `/projects/${E2E_PROJECT_ID}/failures/failure-1/resolve`
    ) {
      state.lastResolutionRequest = request.postDataJSON()
      state.failureResolved = true
      await fulfillJson(route, { failure_id: 'failure-1', resolved: true })
      return
    }

    state.unexpectedRequests.push(requestKey)
    await fulfillJson(
      route,
      { error: { code: 'not_found', message: `Unhandled E2E request: ${requestKey}` } },
      404,
    )
  })

  return state
}
