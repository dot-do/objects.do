/**
 * Event query and CDC routes
 *
 * GET  /events              — query the immutable event log
 * GET  /events/stream       — SSE stream (CDC) for real-time event consumption
 * GET  /events/history/:type/:id — full event history for an entity
 * GET  /events/:id          — get a single event by ID
 *
 * POST /subscriptions       — register a webhook/websocket subscription
 * GET  /subscriptions       — list all subscriptions
 * DELETE /subscriptions/:id — remove a subscription
 *
 * Time travel via entity routes:
 * GET  /entities/:type/:id?asOf=<timestamp>  — reconstruct state at timestamp
 * GET  /entities/:type/:id?atVersion=<n>     — reconstruct state at version
 * GET  /entities/:type/:id/history           — full event history
 * GET  /entities/:type/:id/diff?from=1&to=3  — diff between versions
 */

import { Hono } from 'hono'
import type { AppEnv, ApiResponse } from '../types'

const app = new Hono<AppEnv>()

/**
 * GET /events — query events
 */
app.get('/', async (c) => {
  const tenant = c.get('tenant')
  const doId = c.env.OBJECTS.idFromName(tenant)
  const stub = c.env.OBJECTS.get(doId)

  const url = new URL(c.req.url)
  const qs = url.search

  const res = await stub.fetch(new Request(`https://do/events${qs}`))
  const result = (await res.json()) as ApiResponse
  return c.json(result)
})

/**
 * GET /events/stream — SSE stream (CDC)
 *
 * Query params:
 *   since  — cursor event ID to start after
 *   types  — comma-separated entity types
 *   verbs  — comma-separated verbs
 */
app.get('/stream', async (c) => {
  const tenant = c.get('tenant')
  const doId = c.env.OBJECTS.idFromName(tenant)
  const stub = c.env.OBJECTS.get(doId)

  const url = new URL(c.req.url)
  const qs = url.search

  const res = await stub.fetch(new Request(`https://do/events/stream${qs}`))

  // Forward the SSE response directly
  return new Response(res.body, {
    status: res.status,
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    },
  })
})

/**
 * GET /events/history/:type/:id — entity event history
 */
app.get('/history/:type/:id', async (c) => {
  const tenant = c.get('tenant')
  const doId = c.env.OBJECTS.idFromName(tenant)
  const stub = c.env.OBJECTS.get(doId)

  const entityType = c.req.param('type')
  const entityId = c.req.param('id')

  const res = await stub.fetch(new Request(`https://do/events/history/${entityType}/${entityId}`))
  const result = (await res.json()) as ApiResponse
  return c.json(result)
})

/**
 * GET /events/:id — get single event
 */
app.get('/:id', async (c) => {
  const tenant = c.get('tenant')
  const doId = c.env.OBJECTS.idFromName(tenant)
  const stub = c.env.OBJECTS.get(doId)

  const eventId = c.req.param('id')

  const res = await stub.fetch(new Request(`https://do/events/${eventId}`))
  const result = (await res.json()) as ApiResponse
  return c.json(result)
})

export default app
