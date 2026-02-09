/**
 * Integration hook management routes
 *
 * POST   /integrations/hooks              — register an integration hook
 * GET    /integrations/hooks              — list all hooks (built-in + tenant)
 * DELETE /integrations/hooks/:id          — remove a tenant-configured hook
 * GET    /integrations/dispatch-log       — query dispatch audit log
 */

import { Hono } from 'hono'
import type { AppEnv } from '../types'
import { getStub } from '../lib/tenant'

const app = new Hono<AppEnv>()

/**
 * POST /integrations/hooks — register an integration hook
 */
app.post('/hooks', async (c) => {
  const body = await c.req.json()
  const stub = getStub(c)

  const result = await stub.createIntegrationHook(body)
  return c.json({ success: result.success, data: result.data, error: result.error }, result.status as 200 | 201 | 400)
})

/**
 * GET /integrations/hooks — list all hooks
 */
app.get('/hooks', async (c) => {
  const stub = getStub(c)
  const result = await stub.listIntegrationHooks()
  return c.json(result)
})

/**
 * DELETE /integrations/hooks/:id — delete a hook
 */
app.delete('/hooks/:id', async (c) => {
  const id = c.req.param('id')
  const stub = getStub(c)

  const result = await stub.deleteIntegrationHook(id)
  return c.json({ success: result.success, error: result.error }, result.status as 200 | 403 | 404)
})

/**
 * GET /integrations/dispatch-log — query dispatch audit log
 */
app.get('/dispatch-log', async (c) => {
  const url = new URL(c.req.url)
  const stub = getStub(c)

  const result = await stub.queryDispatchLog({
    eventId: url.searchParams.get('eventId') ?? undefined,
    service: url.searchParams.get('service') ?? undefined,
    status: url.searchParams.get('status') ?? undefined,
    limit: url.searchParams.get('limit') ? parseInt(url.searchParams.get('limit')!, 10) : undefined,
  })

  return c.json(result)
})

export default app
