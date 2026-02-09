/**
 * Subscription management routes
 *
 * POST   /subscriptions       — register a webhook/websocket subscription
 * GET    /subscriptions       — list all subscriptions
 * DELETE /subscriptions/:id   — remove a subscription
 */

import { Hono } from 'hono'
import type { AppEnv, ApiResponse } from '../types'

const app = new Hono<AppEnv>()

/**
 * POST /subscriptions — register a subscription
 */
app.post('/', async (c) => {
  const tenant = c.get('tenant')
  const doId = c.env.OBJECTS.idFromName(tenant)
  const stub = c.env.OBJECTS.get(doId)

  const body = await c.req.text()

  const res = await stub.fetch(
    new Request('https://do/subscriptions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
    }),
  )

  const result = (await res.json()) as ApiResponse
  return c.json(result, res.status as 200 | 201 | 400)
})

/**
 * GET /subscriptions — list all subscriptions
 */
app.get('/', async (c) => {
  const tenant = c.get('tenant')
  const doId = c.env.OBJECTS.idFromName(tenant)
  const stub = c.env.OBJECTS.get(doId)

  const res = await stub.fetch(new Request('https://do/subscriptions'))
  const result = (await res.json()) as ApiResponse
  return c.json(result)
})

/**
 * DELETE /subscriptions/:id — remove a subscription
 */
app.delete('/:id', async (c) => {
  const tenant = c.get('tenant')
  const doId = c.env.OBJECTS.idFromName(tenant)
  const stub = c.env.OBJECTS.get(doId)

  const subId = c.req.param('id')

  const res = await stub.fetch(
    new Request(`https://do/subscriptions/${subId}`, {
      method: 'DELETE',
    }),
  )

  const result = (await res.json()) as ApiResponse
  return c.json(result, res.status as 200 | 404)
})

export default app
