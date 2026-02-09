/**
 * Integration Dispatch — verb-hook event dispatch to .do services
 *
 * When a verb is executed on an entity, checks if integration hooks are
 * registered and dispatches to bound services (PAYMENTS, REPO, EVENTS, etc.).
 *
 * Dispatches are fire-and-forget: they never block the verb response.
 * Results (success or failure) are stored as dispatch events for audit.
 *
 * Pattern:
 *   Contact.qualified  -> PAYMENTS.syncCustomer()
 *   Deal.closed        -> PAYMENTS.createSubscription()
 *   Issue.created      -> REPO.createIssue()
 *
 * Hook configuration is per-tenant, stored in SQLite:
 *   integration_hooks(id, entity_type, verb, service, method, config, active, created_at)
 *
 * Built-in hooks (always-on, no configuration needed):
 *   - CRM -> Payments sync (Contact.qualify, Contact.create, Deal.close)
 *   - Projects -> GitHub sync (Issue.create, Issue.update, Issue.close)
 *
 * Note: CDC event forwarding to events.do is handled by @dotdo/events EventEmitter
 * in ObjectsDO, not via integration hooks.
 */

import type { ServiceBinding } from '../types'

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Registered integration hook (stored in SQLite) */
export interface IntegrationHook {
  id: string
  entityType: string
  verb: string
  /** Which service binding to dispatch to: PAYMENTS | REPO | INTEGRATIONS | OAUTH */
  service: string
  /** HTTP method + path for the service binding RPC call */
  method: string
  /** Optional JSON config (headers, transforms, etc.) */
  config: Record<string, unknown> | null
  active: boolean
  createdAt: string
}

/** Result of a dispatch attempt */
export interface DispatchResult {
  hookId: string
  service: string
  method: string
  status: 'success' | 'error'
  statusCode?: number
  error?: string
  durationMs: number
  timestamp: string
}

/** The event payload sent to service bindings */
export interface DispatchPayload {
  /** The verb event type (e.g., 'Contact.qualified') */
  event: string
  /** Entity type */
  entityType: string
  /** Entity ID */
  entityId: string
  /** The verb that was executed */
  verb: string
  /** Conjugation forms */
  conjugation: { action: string; activity: string; event: string }
  /** Entity state before the verb */
  before: Record<string, unknown> | null
  /** Entity state after the verb */
  after: Record<string, unknown> | null
  /** Verb payload data */
  data: Record<string, unknown> | null
  /** Tenant context URL */
  context: string
  /** When the verb was executed */
  timestamp: string
}

/** Available service binding names */
export type ServiceName = 'PAYMENTS' | 'REPO' | 'INTEGRATIONS' | 'OAUTH'

/** Map of available service bindings */
export type ServiceBindings = Partial<Record<ServiceName, ServiceBinding>>

// ---------------------------------------------------------------------------
// Default / built-in hook mappings
// ---------------------------------------------------------------------------

/**
 * Built-in hooks that are always active regardless of tenant configuration.
 * These represent the core integration contracts between objects.do and .do services.
 *
 * Format: { entityType, verb, service, method }
 * Use '*' for entityType or verb to match any.
 */
export const BUILTIN_HOOKS: ReadonlyArray<{
  entityType: string
  verb: string
  service: ServiceName
  method: string
}> = [
  // NOTE: CDC event forwarding to events.do is now handled by @dotdo/events EventEmitter
  // in ObjectsDO (batched, with circuit breaker and retry). The EVENTS catch-all hook
  // has been removed from here.

  // CRM -> Payments integration
  { entityType: 'Contact', verb: 'qualify', service: 'PAYMENTS', method: 'POST /customers/sync' },
  { entityType: 'Contact', verb: 'create', service: 'PAYMENTS', method: 'POST /customers/sync' },
  { entityType: 'Deal', verb: 'close', service: 'PAYMENTS', method: 'POST /subscriptions/create' },

  // Projects -> GitHub integration
  { entityType: 'Issue', verb: 'create', service: 'REPO', method: 'POST /issues/create' },
  { entityType: 'Issue', verb: 'update', service: 'REPO', method: 'POST /issues/update' },
  { entityType: 'Issue', verb: 'close', service: 'REPO', method: 'POST /issues/close' },
]

// ---------------------------------------------------------------------------
// Matching
// ---------------------------------------------------------------------------

/**
 * Check if a hook matches the given entity type and verb.
 * Supports wildcards ('*') for entityType and verb.
 */
export function hookMatches(hook: { entityType: string; verb: string }, entityType: string, verb: string): boolean {
  const typeMatch = hook.entityType === '*' || hook.entityType === entityType
  const verbMatch = hook.verb === '*' || hook.verb === verb
  return typeMatch && verbMatch
}

/**
 * Find all hooks (built-in + tenant-configured) that match a given entity type and verb.
 */
export function findMatchingHooks(
  entityType: string,
  verb: string,
  tenantHooks: IntegrationHook[],
): Array<{ id: string; service: ServiceName; method: string; config: Record<string, unknown> | null; builtin: boolean }> {
  const matches: Array<{ id: string; service: ServiceName; method: string; config: Record<string, unknown> | null; builtin: boolean }> = []

  // Built-in hooks
  for (const hook of BUILTIN_HOOKS) {
    if (hookMatches(hook, entityType, verb)) {
      matches.push({
        id: `builtin:${hook.service}:${hook.method}`,
        service: hook.service,
        method: hook.method,
        config: null,
        builtin: true,
      })
    }
  }

  // Tenant-configured hooks
  for (const hook of tenantHooks) {
    if (hook.active && hookMatches(hook, entityType, verb)) {
      matches.push({
        id: hook.id,
        service: hook.service as ServiceName,
        method: hook.method,
        config: hook.config,
        builtin: false,
      })
    }
  }

  return matches
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

/**
 * Parse a method string like "POST /customers/sync" into HTTP method and path.
 */
export function parseMethod(method: string): { httpMethod: string; path: string } {
  const spaceIdx = method.indexOf(' ')
  if (spaceIdx === -1) {
    return { httpMethod: 'POST', path: method }
  }
  return {
    httpMethod: method.slice(0, spaceIdx).toUpperCase(),
    path: method.slice(spaceIdx + 1),
  }
}

/**
 * Dispatch a single hook to its service binding.
 * Returns a DispatchResult with timing and status information.
 */
export async function dispatchToService(
  service: ServiceBinding,
  method: string,
  payload: DispatchPayload,
  hookId: string,
  serviceName: string,
): Promise<DispatchResult> {
  const start = Date.now()
  const { httpMethod, path } = parseMethod(method)

  try {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      'X-Headlessly-Event': payload.event,
      'X-Headlessly-Entity-Type': payload.entityType,
      'X-Headlessly-Entity-Id': payload.entityId,
      'X-Headlessly-Verb': payload.verb,
      'X-Headlessly-Hook-Id': hookId,
    }

    // GET/HEAD requests cannot have a body
    const hasBody = httpMethod !== 'GET' && httpMethod !== 'HEAD'
    const response = await service.fetch(
      new Request(`https://internal${path}`, {
        method: httpMethod,
        headers,
        body: hasBody ? JSON.stringify(payload) : undefined,
      }),
    )

    return {
      hookId,
      service: serviceName,
      method,
      status: response.ok ? 'success' : 'error',
      statusCode: response.status,
      error: response.ok ? undefined : `HTTP ${response.status}`,
      durationMs: Date.now() - start,
      timestamp: new Date().toISOString(),
    }
  } catch (err) {
    return {
      hookId,
      service: serviceName,
      method,
      status: 'error',
      error: err instanceof Error ? err.message : 'Unknown error',
      durationMs: Date.now() - start,
      timestamp: new Date().toISOString(),
    }
  }
}

/**
 * Dispatch an event to all matching hooks.
 *
 * This is the main entry point called from ObjectsDO.logEvent().
 * It is fire-and-forget — errors are caught and logged, never thrown.
 *
 * @param services - Map of available service bindings from env
 * @param payload - The event payload to dispatch
 * @param tenantHooks - Tenant-configured integration hooks from SQLite
 * @returns Promise resolving to an array of DispatchResults
 */
export async function dispatchIntegrationHooks(
  services: ServiceBindings,
  payload: DispatchPayload,
  tenantHooks: IntegrationHook[],
): Promise<DispatchResult[]> {
  const hooks = findMatchingHooks(payload.entityType, payload.verb, tenantHooks)
  if (hooks.length === 0) return []

  const results: DispatchResult[] = []

  // Dispatch all hooks concurrently (fire-and-forget pattern)
  const promises = hooks.map(async (hook) => {
    const service = services[hook.service]
    if (!service) {
      // Service binding not available — log and skip
      results.push({
        hookId: hook.id,
        service: hook.service,
        method: hook.method,
        status: 'error',
        error: `Service binding '${hook.service}' not available`,
        durationMs: 0,
        timestamp: new Date().toISOString(),
      })
      return
    }

    const result = await dispatchToService(service, hook.method, payload, hook.id, hook.service)
    results.push(result)
  })

  await Promise.allSettled(promises)

  return results
}
