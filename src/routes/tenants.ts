/**
 * Tenant management routes
 *
 * POST   /tenants          — Create/provision a new tenant
 * GET    /tenants/:id      — Get tenant info
 * GET    /tenants/:id/stats — Tenant statistics (entity counts, event counts)
 * DELETE /tenants/:id      — Deactivate tenant (soft delete)
 */

import { Hono } from 'hono'
import type { AppEnv, ApiResponse } from '../types'
import { getTenantDO } from '../lib/do-router'

const app = new Hono<AppEnv>()

/**
 * POST /tenants — create/provision a new tenant
 *
 * Body: { id: 'acme', name?: 'Acme Corp', plan?: 'pro' }
 *
 * This provisions a tenant DO by sending an initialization request.
 * The DO is lazily created on first access, so this mainly validates
 * the tenant ID and stores metadata.
 */
app.post('/', async (c) => {
  const body = (await c.req.json()) as { id: string; name?: string; plan?: string }

  if (!body.id || typeof body.id !== 'string') {
    return c.json<ApiResponse>({ success: false, error: 'Missing or invalid tenant id' }, 400)
  }

  // Validate tenant ID format: lowercase alphanumeric + hyphens, 2-64 chars
  if (!/^[a-z0-9][a-z0-9-]{0,62}[a-z0-9]$/.test(body.id) && body.id.length < 2) {
    return c.json<ApiResponse>({ success: false, error: 'Tenant ID must be 2-64 lowercase alphanumeric characters or hyphens' }, 400)
  }

  const stub = getTenantDO(c.env, body.id)

  // Send a provision request to the DO
  const res = await stub.fetch(
    new Request('https://do/tenant/provision', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Tenant-ID': body.id,
      },
      body: JSON.stringify({
        tenantId: body.id,
        name: body.name,
        plan: body.plan,
      }),
    }),
  )

  const result = (await res.json()) as ApiResponse
  return c.json(result, res.status as 200 | 201 | 400 | 409 | 500)
})

/**
 * GET /tenants/:id — get tenant info
 */
app.get('/:id', async (c) => {
  const id = c.req.param('id')
  const stub = getTenantDO(c.env, id)

  const res = await stub.fetch(
    new Request('https://do/tenant/info', {
      headers: { 'X-Tenant-ID': id },
    }),
  )

  const result = (await res.json()) as ApiResponse
  return c.json(result, res.status as 200 | 404)
})

/**
 * GET /tenants/:id/stats — tenant statistics
 */
app.get('/:id/stats', async (c) => {
  const id = c.req.param('id')
  const stub = getTenantDO(c.env, id)

  const res = await stub.fetch(
    new Request('https://do/tenant/stats', {
      headers: { 'X-Tenant-ID': id },
    }),
  )

  const result = (await res.json()) as ApiResponse
  return c.json(result, res.status as 200 | 404)
})

/**
 * DELETE /tenants/:id — deactivate tenant (soft delete)
 */
app.delete('/:id', async (c) => {
  const id = c.req.param('id')
  const stub = getTenantDO(c.env, id)

  const res = await stub.fetch(
    new Request('https://do/tenant/deactivate', {
      method: 'DELETE',
      headers: { 'X-Tenant-ID': id },
    }),
  )

  const result = (await res.json()) as ApiResponse
  return c.json(result, res.status as 200 | 404)
})

export default app
