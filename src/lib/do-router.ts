/**
 * DO Router — routes requests to the correct tenant Durable Object
 *
 * Each tenant gets a deterministic DO ID derived from their tenant name
 * via `idFromName()`. This ensures the same tenant always maps to the
 * same DO instance, providing complete data isolation (each DO has its
 * own SQLite database).
 */

import type { TenantContext } from '../types'

/**
 * Get the DurableObjectStub for a given tenant.
 *
 * The DO ID is deterministic: `idFromName(tenantId)` always returns
 * the same ID for the same tenant, so the same DO instance handles
 * all requests for that tenant.
 */
export function getTenantDO(env: { OBJECTS: DurableObjectNamespace }, tenantId: string): DurableObjectStub {
  const id = env.OBJECTS.idFromName(tenantId)
  return env.OBJECTS.get(id)
}

/**
 * Forward a request to the tenant's Durable Object.
 *
 * Adds tenant context headers so the DO knows which tenant it's serving
 * and what subdomain context (journey/system/industry) is active.
 *
 * The DO URL is rewritten to `https://do/...` because the DO's fetch()
 * handler only looks at the pathname — the hostname is irrelevant.
 */
export async function forwardToTenant(
  env: { OBJECTS: DurableObjectNamespace },
  tenantCtx: TenantContext,
  request: Request,
  pathOverride?: string,
): Promise<Response> {
  const stub = getTenantDO(env, tenantCtx.tenantId)

  const url = new URL(request.url)
  const path = pathOverride ?? url.pathname
  const qs = url.search

  // Build headers with tenant context
  const headers = new Headers(request.headers)
  headers.set('X-Tenant-ID', tenantCtx.tenantId)
  headers.set('X-Context-URL', tenantCtx.contextUrl)
  if (tenantCtx.journey) headers.set('X-Journey', tenantCtx.journey)
  if (tenantCtx.system) headers.set('X-System', tenantCtx.system)
  if (tenantCtx.industry) headers.set('X-Industry', tenantCtx.industry)

  // Forward to the DO with a synthetic URL (DO only cares about the path)
  return stub.fetch(
    new Request(`https://do${path}${qs}`, {
      method: request.method,
      headers,
      body: request.body,
    }),
  )
}
