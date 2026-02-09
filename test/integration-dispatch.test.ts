/**
 * Tests for the integration dispatch system
 *
 * Verifies:
 * - Hook matching (wildcards, exact matches)
 * - Built-in hook resolution
 * - Tenant hook resolution
 * - Service dispatch (success and error handling)
 * - Method parsing
 * - Fire-and-forget behavior (errors don't propagate)
 * - Dispatch payload structure
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  hookMatches,
  findMatchingHooks,
  parseMethod,
  dispatchToService,
  dispatchIntegrationHooks,
  BUILTIN_HOOKS,
  type IntegrationHook,
  type DispatchPayload,
  type ServiceBindings,
} from '../src/lib/integration-dispatch'

// ---------------------------------------------------------------------------
// hookMatches
// ---------------------------------------------------------------------------

describe('hookMatches', () => {
  it('should match exact entity type and verb', () => {
    expect(hookMatches({ entityType: 'Contact', verb: 'qualify' }, 'Contact', 'qualify')).toBe(true)
  })

  it('should not match different entity type', () => {
    expect(hookMatches({ entityType: 'Contact', verb: 'qualify' }, 'Deal', 'qualify')).toBe(false)
  })

  it('should not match different verb', () => {
    expect(hookMatches({ entityType: 'Contact', verb: 'qualify' }, 'Contact', 'create')).toBe(false)
  })

  it('should match wildcard entity type', () => {
    expect(hookMatches({ entityType: '*', verb: 'create' }, 'Contact', 'create')).toBe(true)
    expect(hookMatches({ entityType: '*', verb: 'create' }, 'Deal', 'create')).toBe(true)
  })

  it('should match wildcard verb', () => {
    expect(hookMatches({ entityType: 'Contact', verb: '*' }, 'Contact', 'create')).toBe(true)
    expect(hookMatches({ entityType: 'Contact', verb: '*' }, 'Contact', 'qualify')).toBe(true)
  })

  it('should match double wildcard (catch-all)', () => {
    expect(hookMatches({ entityType: '*', verb: '*' }, 'Contact', 'create')).toBe(true)
    expect(hookMatches({ entityType: '*', verb: '*' }, 'Deal', 'close')).toBe(true)
    expect(hookMatches({ entityType: '*', verb: '*' }, 'Issue', 'update')).toBe(true)
  })

  it('should not match wildcard entity type with different verb', () => {
    expect(hookMatches({ entityType: '*', verb: 'create' }, 'Contact', 'qualify')).toBe(false)
  })

  it('should not match wildcard verb with different entity type', () => {
    expect(hookMatches({ entityType: 'Contact', verb: '*' }, 'Deal', 'create')).toBe(false)
  })
})

// ---------------------------------------------------------------------------
// parseMethod
// ---------------------------------------------------------------------------

describe('parseMethod', () => {
  it('should parse "POST /customers/sync"', () => {
    const { httpMethod, path } = parseMethod('POST /customers/sync')
    expect(httpMethod).toBe('POST')
    expect(path).toBe('/customers/sync')
  })

  it('should parse "GET /issues/list"', () => {
    const { httpMethod, path } = parseMethod('GET /issues/list')
    expect(httpMethod).toBe('GET')
    expect(path).toBe('/issues/list')
  })

  it('should parse "PUT /entities/update"', () => {
    const { httpMethod, path } = parseMethod('PUT /entities/update')
    expect(httpMethod).toBe('PUT')
    expect(path).toBe('/entities/update')
  })

  it('should default to POST if no method prefix', () => {
    const { httpMethod, path } = parseMethod('/customers/sync')
    expect(httpMethod).toBe('POST')
    expect(path).toBe('/customers/sync')
  })

  it('should uppercase the method', () => {
    const { httpMethod, path } = parseMethod('post /events/ingest')
    expect(httpMethod).toBe('POST')
    expect(path).toBe('/events/ingest')
  })
})

// ---------------------------------------------------------------------------
// BUILTIN_HOOKS
// ---------------------------------------------------------------------------

describe('BUILTIN_HOOKS', () => {
  it('should have the catch-all EVENTS hook', () => {
    const eventsHook = BUILTIN_HOOKS.find((h) => h.service === 'EVENTS' && h.entityType === '*' && h.verb === '*')
    expect(eventsHook).toBeDefined()
    expect(eventsHook!.method).toBe('POST /events/ingest')
  })

  it('should have Contact.qualify -> PAYMENTS hook', () => {
    const hook = BUILTIN_HOOKS.find((h) => h.entityType === 'Contact' && h.verb === 'qualify' && h.service === 'PAYMENTS')
    expect(hook).toBeDefined()
    expect(hook!.method).toBe('POST /customers/sync')
  })

  it('should have Deal.close -> PAYMENTS hook', () => {
    const hook = BUILTIN_HOOKS.find((h) => h.entityType === 'Deal' && h.verb === 'close' && h.service === 'PAYMENTS')
    expect(hook).toBeDefined()
    expect(hook!.method).toBe('POST /subscriptions/create')
  })

  it('should have Issue.create -> REPO hook', () => {
    const hook = BUILTIN_HOOKS.find((h) => h.entityType === 'Issue' && h.verb === 'create' && h.service === 'REPO')
    expect(hook).toBeDefined()
    expect(hook!.method).toBe('POST /issues/create')
  })

  it('should have Issue.update -> REPO hook', () => {
    const hook = BUILTIN_HOOKS.find((h) => h.entityType === 'Issue' && h.verb === 'update' && h.service === 'REPO')
    expect(hook).toBeDefined()
  })

  it('should have Issue.close -> REPO hook', () => {
    const hook = BUILTIN_HOOKS.find((h) => h.entityType === 'Issue' && h.verb === 'close' && h.service === 'REPO')
    expect(hook).toBeDefined()
  })
})

// ---------------------------------------------------------------------------
// findMatchingHooks
// ---------------------------------------------------------------------------

describe('findMatchingHooks', () => {
  const tenantHooks: IntegrationHook[] = [
    {
      id: 'ihook_custom1',
      entityType: 'Subscription',
      verb: 'cancel',
      service: 'PAYMENTS',
      method: 'POST /subscriptions/cancel',
      config: null,
      active: true,
      createdAt: '2025-01-01T00:00:00.000Z',
    },
    {
      id: 'ihook_custom2',
      entityType: 'Contact',
      verb: 'create',
      service: 'INTEGRATIONS',
      method: 'POST /contacts/onboard',
      config: { sendWelcomeEmail: true },
      active: true,
      createdAt: '2025-01-01T00:00:00.000Z',
    },
    {
      id: 'ihook_inactive',
      entityType: '*',
      verb: '*',
      service: 'INTEGRATIONS',
      method: 'POST /catch-all',
      config: null,
      active: false,
      createdAt: '2025-01-01T00:00:00.000Z',
    },
  ]

  it('should return built-in EVENTS hook for any entity and verb', () => {
    const hooks = findMatchingHooks('Whatever', 'whatever', [])
    const eventsHook = hooks.find((h) => h.service === 'EVENTS')
    expect(eventsHook).toBeDefined()
    expect(eventsHook!.builtin).toBe(true)
  })

  it('should return built-in PAYMENTS hook for Contact.qualify', () => {
    const hooks = findMatchingHooks('Contact', 'qualify', [])
    const paymentsHook = hooks.find((h) => h.service === 'PAYMENTS')
    expect(paymentsHook).toBeDefined()
    expect(paymentsHook!.builtin).toBe(true)
    expect(paymentsHook!.method).toBe('POST /customers/sync')
  })

  it('should return both built-in and tenant hooks for Contact.create', () => {
    const hooks = findMatchingHooks('Contact', 'create', tenantHooks)

    // Built-in: EVENTS (catch-all) + PAYMENTS (Contact.create)
    const builtinHooks = hooks.filter((h) => h.builtin)
    expect(builtinHooks.length).toBeGreaterThanOrEqual(2)

    // Tenant: ihook_custom2
    const tenantMatches = hooks.filter((h) => !h.builtin)
    expect(tenantMatches.length).toBe(1)
    expect(tenantMatches[0]!.id).toBe('ihook_custom2')
    expect(tenantMatches[0]!.config).toEqual({ sendWelcomeEmail: true })
  })

  it('should return tenant Subscription.cancel hook', () => {
    const hooks = findMatchingHooks('Subscription', 'cancel', tenantHooks)
    const cancelHook = hooks.find((h) => h.id === 'ihook_custom1')
    expect(cancelHook).toBeDefined()
    expect(cancelHook!.service).toBe('PAYMENTS')
  })

  it('should not return inactive tenant hooks', () => {
    const hooks = findMatchingHooks('Something', 'anything', tenantHooks)
    const inactiveHook = hooks.find((h) => h.id === 'ihook_inactive')
    expect(inactiveHook).toBeUndefined()
  })

  it('should not return tenant hooks for non-matching types', () => {
    const hooks = findMatchingHooks('Deal', 'close', tenantHooks)
    const tenantMatches = hooks.filter((h) => !h.builtin)
    expect(tenantMatches.length).toBe(0)
  })
})

// ---------------------------------------------------------------------------
// dispatchToService
// ---------------------------------------------------------------------------

describe('dispatchToService', () => {
  const payload: DispatchPayload = {
    event: 'Contact.qualify',
    entityType: 'Contact',
    entityId: 'contact_abc123',
    verb: 'qualify',
    conjugation: { action: 'qualify', activity: 'qualifying', event: 'qualified' },
    before: { $id: 'contact_abc123', $type: 'Contact', stage: 'Lead' },
    after: { $id: 'contact_abc123', $type: 'Contact', stage: 'Qualified' },
    data: { stage: 'Qualified' },
    context: 'https://headless.ly/~acme',
    timestamp: '2025-01-15T10:30:00.000Z',
  }

  it('should dispatch successfully and return success result', async () => {
    const mockService = {
      fetch: vi.fn().mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200 })),
    }

    const result = await dispatchToService(mockService, 'POST /customers/sync', payload, 'hook_1', 'PAYMENTS')

    expect(result.status).toBe('success')
    expect(result.hookId).toBe('hook_1')
    expect(result.service).toBe('PAYMENTS')
    expect(result.method).toBe('POST /customers/sync')
    expect(result.statusCode).toBe(200)
    expect(result.durationMs).toBeGreaterThanOrEqual(0)
    expect(result.error).toBeUndefined()

    // Verify the request was constructed correctly
    const [fetchRequest] = mockService.fetch.mock.calls[0]!
    expect(fetchRequest.method).toBe('POST')
    expect(new URL(fetchRequest.url).pathname).toBe('/customers/sync')
    expect(fetchRequest.headers.get('Content-Type')).toBe('application/json')
    expect(fetchRequest.headers.get('X-Headlessly-Event')).toBe('Contact.qualify')
    expect(fetchRequest.headers.get('X-Headlessly-Entity-Type')).toBe('Contact')
    expect(fetchRequest.headers.get('X-Headlessly-Entity-Id')).toBe('contact_abc123')
    expect(fetchRequest.headers.get('X-Headlessly-Verb')).toBe('qualify')
    expect(fetchRequest.headers.get('X-Headlessly-Hook-Id')).toBe('hook_1')

    // Verify body
    const body = await fetchRequest.json()
    expect(body.event).toBe('Contact.qualify')
    expect(body.entityType).toBe('Contact')
    expect(body.entityId).toBe('contact_abc123')
    expect(body.verb).toBe('qualify')
    expect(body.context).toBe('https://headless.ly/~acme')
  })

  it('should return error result for non-OK response', async () => {
    const mockService = {
      fetch: vi.fn().mockResolvedValue(new Response('Not Found', { status: 404 })),
    }

    const result = await dispatchToService(mockService, 'POST /customers/sync', payload, 'hook_2', 'PAYMENTS')

    expect(result.status).toBe('error')
    expect(result.statusCode).toBe(404)
    expect(result.error).toBe('HTTP 404')
    expect(result.durationMs).toBeGreaterThanOrEqual(0)
  })

  it('should return error result for network failure', async () => {
    const mockService = {
      fetch: vi.fn().mockRejectedValue(new Error('Connection refused')),
    }

    const result = await dispatchToService(mockService, 'POST /customers/sync', payload, 'hook_3', 'PAYMENTS')

    expect(result.status).toBe('error')
    expect(result.error).toBe('Connection refused')
    expect(result.statusCode).toBeUndefined()
  })

  it('should handle non-Error throws gracefully', async () => {
    const mockService = {
      fetch: vi.fn().mockRejectedValue('something weird'),
    }

    const result = await dispatchToService(mockService, 'POST /test', payload, 'hook_4', 'REPO')

    expect(result.status).toBe('error')
    expect(result.error).toBe('Unknown error')
  })

  it('should use GET method when specified', async () => {
    const mockService = {
      fetch: vi.fn().mockResolvedValue(new Response('OK', { status: 200 })),
    }

    await dispatchToService(mockService, 'GET /status', payload, 'hook_5', 'EVENTS')

    expect(mockService.fetch).toHaveBeenCalledTimes(1)
    const fetchRequest = mockService.fetch.mock.calls[0]![0] as Request
    expect(fetchRequest.method).toBe('GET')
  })
})

// ---------------------------------------------------------------------------
// dispatchIntegrationHooks
// ---------------------------------------------------------------------------

describe('dispatchIntegrationHooks', () => {
  const payload: DispatchPayload = {
    event: 'Contact.create',
    entityType: 'Contact',
    entityId: 'contact_xyz',
    verb: 'create',
    conjugation: { action: 'create', activity: 'creating', event: 'created' },
    before: null,
    after: { $id: 'contact_xyz', $type: 'Contact', name: 'Alice' },
    data: { name: 'Alice' },
    context: 'https://headless.ly/~acme',
    timestamp: '2025-01-15T10:30:00.000Z',
  }

  it('should report errors for hooks when no matching service bindings exist', async () => {
    const results = await dispatchIntegrationHooks({}, payload, [])
    // Built-in hooks still match (EVENTS catch-all, PAYMENTS for Contact.create)
    // but since no services are bound, they all report "not available" errors
    expect(results.length).toBeGreaterThan(0)
    for (const result of results) {
      expect(result.status).toBe('error')
      expect(result.error).toContain('not available')
    }
  })

  it('should dispatch to EVENTS service for any event (catch-all)', async () => {
    const eventsService = {
      fetch: vi.fn().mockResolvedValue(new Response('OK', { status: 200 })),
    }

    const services: ServiceBindings = { EVENTS: eventsService }
    const results = await dispatchIntegrationHooks(services, payload, [])

    // Should have at least the EVENTS catch-all hook
    const eventsResult = results.find((r) => r.service === 'EVENTS')
    expect(eventsResult).toBeDefined()
    expect(eventsResult!.status).toBe('success')
  })

  it('should dispatch to PAYMENTS for Contact.create (built-in)', async () => {
    const paymentsService = {
      fetch: vi.fn().mockResolvedValue(new Response('OK', { status: 200 })),
    }
    const eventsService = {
      fetch: vi.fn().mockResolvedValue(new Response('OK', { status: 200 })),
    }

    const services: ServiceBindings = { PAYMENTS: paymentsService, EVENTS: eventsService }
    const results = await dispatchIntegrationHooks(services, payload, [])

    const paymentsResult = results.find((r) => r.service === 'PAYMENTS')
    expect(paymentsResult).toBeDefined()
    expect(paymentsResult!.status).toBe('success')
  })

  it('should dispatch to tenant-configured hooks', async () => {
    const integrationsService = {
      fetch: vi.fn().mockResolvedValue(new Response('OK', { status: 200 })),
    }
    const eventsService = {
      fetch: vi.fn().mockResolvedValue(new Response('OK', { status: 200 })),
    }

    const services: ServiceBindings = { INTEGRATIONS: integrationsService, EVENTS: eventsService }

    const tenantHooks: IntegrationHook[] = [
      {
        id: 'ihook_tenant1',
        entityType: 'Contact',
        verb: 'create',
        service: 'INTEGRATIONS',
        method: 'POST /contacts/onboard',
        config: null,
        active: true,
        createdAt: '2025-01-01T00:00:00.000Z',
      },
    ]

    const results = await dispatchIntegrationHooks(services, payload, tenantHooks)

    const tenantResult = results.find((r) => r.hookId === 'ihook_tenant1')
    expect(tenantResult).toBeDefined()
    expect(tenantResult!.service).toBe('INTEGRATIONS')
    expect(tenantResult!.status).toBe('success')
  })

  it('should record error when service binding is not available', async () => {
    // Only EVENTS is available, but Contact.create also needs PAYMENTS
    const eventsService = {
      fetch: vi.fn().mockResolvedValue(new Response('OK', { status: 200 })),
    }

    const services: ServiceBindings = { EVENTS: eventsService }
    const results = await dispatchIntegrationHooks(services, payload, [])

    // PAYMENTS should have an error result because the binding is not available
    const paymentsResult = results.find((r) => r.service === 'PAYMENTS')
    expect(paymentsResult).toBeDefined()
    expect(paymentsResult!.status).toBe('error')
    expect(paymentsResult!.error).toContain('not available')
  })

  it('should dispatch to REPO for Issue.create', async () => {
    const repoService = {
      fetch: vi.fn().mockResolvedValue(new Response('OK', { status: 200 })),
    }
    const eventsService = {
      fetch: vi.fn().mockResolvedValue(new Response('OK', { status: 200 })),
    }

    const issuePayload: DispatchPayload = {
      ...payload,
      event: 'Issue.create',
      entityType: 'Issue',
      entityId: 'issue_123',
    }

    const services: ServiceBindings = { REPO: repoService, EVENTS: eventsService }
    const results = await dispatchIntegrationHooks(services, issuePayload, [])

    const repoResult = results.find((r) => r.service === 'REPO')
    expect(repoResult).toBeDefined()
    expect(repoResult!.status).toBe('success')
    expect(repoResult!.method).toBe('POST /issues/create')
  })

  it('should handle partial failures gracefully', async () => {
    const paymentsService = {
      fetch: vi.fn().mockRejectedValue(new Error('Payment service down')),
    }
    const eventsService = {
      fetch: vi.fn().mockResolvedValue(new Response('OK', { status: 200 })),
    }

    const services: ServiceBindings = { PAYMENTS: paymentsService, EVENTS: eventsService }
    const results = await dispatchIntegrationHooks(services, payload, [])

    // Events should succeed
    const eventsResult = results.find((r) => r.service === 'EVENTS')
    expect(eventsResult!.status).toBe('success')

    // Payments should fail but not throw
    const paymentsResult = results.find((r) => r.service === 'PAYMENTS')
    expect(paymentsResult!.status).toBe('error')
    expect(paymentsResult!.error).toBe('Payment service down')
  })

  it('should dispatch Deal.close to PAYMENTS with createSubscription', async () => {
    const paymentsService = {
      fetch: vi.fn().mockResolvedValue(new Response('OK', { status: 200 })),
    }
    const eventsService = {
      fetch: vi.fn().mockResolvedValue(new Response('OK', { status: 200 })),
    }

    const dealPayload: DispatchPayload = {
      event: 'Deal.close',
      entityType: 'Deal',
      entityId: 'deal_abc',
      verb: 'close',
      conjugation: { action: 'close', activity: 'closing', event: 'closed' },
      before: { $id: 'deal_abc', $type: 'Deal', status: 'Open', value: 50000 },
      after: { $id: 'deal_abc', $type: 'Deal', status: 'Won', value: 50000 },
      data: { status: 'Won' },
      context: 'https://headless.ly/~acme',
      timestamp: '2025-01-15T10:30:00.000Z',
    }

    const services: ServiceBindings = { PAYMENTS: paymentsService, EVENTS: eventsService }
    const results = await dispatchIntegrationHooks(services, dealPayload, [])

    const paymentsResult = results.find((r) => r.service === 'PAYMENTS')
    expect(paymentsResult).toBeDefined()
    expect(paymentsResult!.method).toBe('POST /subscriptions/create')
    expect(paymentsResult!.status).toBe('success')
  })

  it('should not dispatch to non-matching hooks', async () => {
    const eventsService = {
      fetch: vi.fn().mockResolvedValue(new Response('OK', { status: 200 })),
    }

    const services: ServiceBindings = { EVENTS: eventsService }

    const projectPayload: DispatchPayload = {
      ...payload,
      event: 'Project.archive',
      entityType: 'Project',
      entityId: 'project_1',
      verb: 'archive',
    }

    const results = await dispatchIntegrationHooks(services, projectPayload, [])

    // Only EVENTS catch-all should match, not PAYMENTS or REPO
    const nonEventsResults = results.filter((r) => r.service !== 'EVENTS')
    expect(nonEventsResults.length).toBe(0)
  })
})
