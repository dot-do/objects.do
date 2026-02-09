/**
 * Subscription management routes
 *
 * POST   /subscriptions       — register a webhook/websocket subscription
 * GET    /subscriptions       — list all subscriptions
 * DELETE /subscriptions/:id   — remove a subscription
 */

import { Hono } from 'hono'
import type { AppEnv } from '../types'
import { getStub } from '../lib/tenant'

const app = new Hono<AppEnv>()

/**
 * POST /subscriptions — register a subscription
 */
app.post('/', async (c) => {
  const body = await c.req.json()
  const stub = getStub(c)

  const result = await stub.createSubscription(body)
  return c.json({ success: result.success, data: result.data, error: result.error }, result.status as 200 | 201 | 400)
})

/**
 * GET /subscriptions — list all subscriptions
 */
app.get('/', async (c) => {
  const stub = getStub(c)
  const result = await stub.listSubscriptions()
  return c.json(result)
})

/**
 * DELETE /subscriptions/:id — remove a subscription
 */
app.delete('/:id', async (c) => {
  const subId = c.req.param('id')
  const stub = getStub(c)

  const result = await stub.deleteSubscription(subId)
  return c.json({ success: result.success, error: result.error }, result.status as 200 | 404)
})

export default app
