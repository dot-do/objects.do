/**
 * Integration hook management routes
 *
 * POST   /integrations/hooks              — register an integration hook
 * GET    /integrations/hooks              — list all hooks (built-in + tenant)
 * DELETE /integrations/hooks/:id          — remove a tenant-configured hook
 * GET    /integrations/dispatch-log       — query dispatch audit log
 */

import { Hono } from 'hono'
import type { AppEnv, ApiResponse } from '../types'

const app = new Hono<AppEnv>()

/**
 * Forward a request to the ObjectsDO for the current tenant
 */
async function forward(c: { env: AppEnv['Bindings']; get: (key: 'tenant') => string }, path: string, init?: RequestInit): Promise<Response> {
  const tenant = c.get('tenant')
  const doId = c.env.OBJECTS.idFromName(tenant)
  const stub = c.env.OBJECTS.get(doId)
  const headers = new Headers(init?.headers)
  headers.set('X-Tenant', tenant)
  return stub.fetch(new Request(`https://do${path}`, { ...init, headers }))
}

/**
 * POST /integrations/hooks — register an integration hook
 */
app.post('/hooks', async (c) => {
  const body = await c.req.text()

  const res = await forward(c, '/integrations/hooks', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body,
  })

  const result = (await res.json()) as ApiResponse
  return c.json(result, res.status as 200 | 201 | 400)
})

/**
 * GET /integrations/hooks — list all hooks
 */
app.get('/hooks', async (c) => {
  const res = await forward(c, '/integrations/hooks')
  const result = (await res.json()) as ApiResponse
  return c.json(result, res.status as 200)
})

/**
 * DELETE /integrations/hooks/:id — delete a hook
 */
app.delete('/hooks/:id', async (c) => {
  const id = c.req.param('id')
  const res = await forward(c, `/integrations/hooks/${id}`, { method: 'DELETE' })
  const result = (await res.json()) as ApiResponse
  return c.json(result, res.status as 200 | 403 | 404)
})

/**
 * GET /integrations/dispatch-log — query dispatch audit log
 */
app.get('/dispatch-log', async (c) => {
  const url = new URL(c.req.url)
  const qs = url.search
  const res = await forward(c, `/integrations/dispatch-log${qs}`)
  const result = (await res.json()) as ApiResponse
  return c.json(result, res.status as 200)
})

export default app
