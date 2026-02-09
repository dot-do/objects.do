/**
 * Event query and CDC routes
 *
 * GET  /events              — query the immutable event log
 * GET  /events/stream       — SSE stream (CDC) for real-time event consumption
 * GET  /events/history/:type/:id — full event history for an entity
 * GET  /events/:id          — get a single event by ID
 */

import { Hono } from 'hono'
import type { AppEnv } from '../types'
import { getStub } from '../lib/tenant'

const app = new Hono<AppEnv>()

/**
 * GET /events — query events
 */
app.get('/', async (c) => {
  const stub = getStub(c)
  const url = new URL(c.req.url)

  const result = await stub.queryEvents({
    since: url.searchParams.get('since') ?? undefined,
    type: url.searchParams.get('type') ?? undefined,
    entityId: url.searchParams.get('entityId') ?? undefined,
    verb: url.searchParams.get('verb') ?? undefined,
    limit: url.searchParams.get('limit') ? parseInt(url.searchParams.get('limit')!, 10) : undefined,
  })

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
  const stub = getStub(c)
  const url = new URL(c.req.url)

  const res = await stub.getEventStream({
    since: url.searchParams.get('since') ?? undefined,
    types: url.searchParams.get('types') ?? undefined,
    verbs: url.searchParams.get('verbs') ?? undefined,
  })

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
  const stub = getStub(c)
  const entityType = c.req.param('type')
  const entityId = c.req.param('id')

  const result = await stub.entityHistory(entityType, entityId)
  return c.json(result)
})

/**
 * GET /events/:id — get single event
 */
app.get('/:id', async (c) => {
  const stub = getStub(c)
  const eventId = c.req.param('id')

  const result = await stub.getEvent(eventId)
  return c.json({ success: result.success, data: result.data, error: result.error }, result.status as 200 | 404)
})

export default app
