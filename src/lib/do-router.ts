/**
 * DO Router — resolves the correct tenant Durable Object stub
 *
 * Each tenant gets a deterministic DO ID derived from their tenant name
 * via `idFromName()`. This ensures the same tenant always maps to the
 * same DO instance, providing complete data isolation (each DO has its
 * own SQLite database).
 *
 * The returned stub is cast to ObjectsStub for direct RPC method calls.
 */

import type { ObjectsStub } from '../types'

/**
 * Get the typed DurableObjectStub for a given tenant.
 *
 * The DO ID is deterministic: `idFromName(tenantId)` always returns
 * the same ID for the same tenant, so the same DO instance handles
 * all requests for that tenant.
 */
export function getTenantStub(env: { OBJECTS: DurableObjectNamespace }, tenantId: string): ObjectsStub {
  const id = env.OBJECTS.idFromName(tenantId)
  return env.OBJECTS.get(id) as unknown as ObjectsStub
}
