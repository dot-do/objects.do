/**
 * Entity CRUD + verb execution routes
 *
 * POST   /entities/:type              — create entity
 * GET    /entities/:type              — list/find entities (filter, limit, offset, sort)
 * GET    /entities/:type/:id          — get entity by ID
 * PUT    /entities/:type/:id          — update entity
 * DELETE /entities/:type/:id          — soft delete entity
 * POST   /entities/:type/:id/:verb    — execute verb (e.g., POST /entities/Contact/contact_abc/qualify)
 * POST   /entities/:type/hooks        — register hook { verb, phase, code }
 */

import { Hono } from 'hono'
import type { AppEnv } from '../types'
import { getStub } from '../lib/tenant'

const app = new Hono<AppEnv>()

/**
 * POST /entities/:type — create a new entity
 */
app.post('/:type', async (c) => {
  const type = c.req.param('type')
  const body = await c.req.json()
  const stub = getStub(c)
  const tenantCtx = c.get('tenantContext')

  const result = await stub.createEntity(type, body, {
    tenantId: tenantCtx?.tenantId,
    contextUrl: tenantCtx?.contextUrl,
  })

  return c.json({ success: result.success, data: result.data, error: result.error, meta: result.meta }, result.status as 200 | 201 | 400 | 403 | 500)
})

/**
 * GET /entities/:type — list/find entities
 *
 * Query params: filter (JSON), limit, offset, sort (JSON)
 */
app.get('/:type', async (c) => {
  const type = c.req.param('type')
  const url = new URL(c.req.url)
  const stub = getStub(c)

  const result = await stub.listEntities(type, {
    limit: url.searchParams.get('limit') ? parseInt(url.searchParams.get('limit')!, 10) : undefined,
    offset: url.searchParams.get('offset') ? parseInt(url.searchParams.get('offset')!, 10) : undefined,
    filter: url.searchParams.get('filter') ?? undefined,
    sort: url.searchParams.get('sort') ?? undefined,
  })

  return c.json({ success: result.success, data: result.data, error: result.error, meta: result.meta }, result.status as 200 | 400)
})

/**
 * GET /entities/:type/:id — get entity by ID
 *
 * Supports time travel via asOf/atVersion query params, history, and diff.
 */
app.get('/:type/:id/history', async (c) => {
  const type = c.req.param('type')
  const id = c.req.param('id')
  const stub = getStub(c)

  const result = await stub.entityHistory(type, id)
  return c.json(result)
})

app.get('/:type/:id/diff', async (c) => {
  const type = c.req.param('type')
  const id = c.req.param('id')
  const url = new URL(c.req.url)
  const stub = getStub(c)

  const result = await stub.entityDiff(type, id, {
    from: url.searchParams.get('from') ?? undefined,
    to: url.searchParams.get('to') ?? undefined,
  })

  return c.json({ success: result.success, data: result.data, error: result.error }, result.status as 200 | 400 | 404)
})

app.get('/:type/:id', async (c) => {
  const type = c.req.param('type')
  const id = c.req.param('id')
  const url = new URL(c.req.url)
  const stub = getStub(c)

  const asOf = url.searchParams.get('asOf')
  const atVersion = url.searchParams.get('atVersion')

  if (asOf || atVersion) {
    const result = await stub.timeTravelGet(type, id, {
      asOf: asOf ?? undefined,
      atVersion: atVersion ?? undefined,
    })
    return c.json({ success: result.success, data: result.data, error: result.error }, result.status as 200 | 400 | 404)
  }

  const result = await stub.getEntity(type, id)
  if (result.etag) c.header('ETag', result.etag)
  return c.json({ success: result.success, data: result.data, error: result.error }, result.status as 200 | 404)
})

/**
 * PUT /entities/:type/:id — update entity
 */
app.put('/:type/:id', async (c) => {
  const type = c.req.param('type')
  const id = c.req.param('id')
  const body = await c.req.json()
  const stub = getStub(c)
  const ifMatch = c.req.header('If-Match')

  const result = await stub.updateEntity(type, id, body, ifMatch ? { ifMatch } : undefined)
  if (result.etag) c.header('ETag', result.etag)
  return c.json({ success: result.success, data: result.data, error: result.error, meta: result.meta }, result.status as 200 | 404 | 409 | 500)
})

/**
 * PATCH /entities/:type/:id — partial update (alias for PUT)
 */
app.patch('/:type/:id', async (c) => {
  const type = c.req.param('type')
  const id = c.req.param('id')
  const body = await c.req.json()
  const stub = getStub(c)
  const ifMatch = c.req.header('If-Match')

  const result = await stub.updateEntity(type, id, body, ifMatch ? { ifMatch } : undefined)
  if (result.etag) c.header('ETag', result.etag)
  return c.json({ success: result.success, data: result.data, error: result.error, meta: result.meta }, result.status as 200 | 404 | 409 | 500)
})

/**
 * DELETE /entities/:type/:id — soft delete entity
 */
app.delete('/:type/:id', async (c) => {
  const type = c.req.param('type')
  const id = c.req.param('id')
  const stub = getStub(c)

  const result = await stub.deleteEntity(type, id)
  return c.json({ success: result.success, error: result.error, meta: result.meta }, result.status as 200 | 403 | 404)
})

/**
 * POST /entities/:type/:id/:verb — execute a verb
 *
 * e.g., POST /entities/Contact/contact_abc/qualify
 * Body: optional verb payload (merged into entity)
 */
app.post('/:type/:id/:verb', async (c) => {
  const type = c.req.param('type')
  const id = c.req.param('id')
  const verb = c.req.param('verb')
  const stub = getStub(c)

  // Special case: "hooks" is not a verb — route to hook registration
  if (verb === 'hooks') {
    const body = await c.req.json()
    const result = await stub.registerHook(type, body)
    return c.json({ success: result.success, data: result.data, error: result.error }, result.status as 200 | 201 | 400)
  }

  let verbData: Record<string, unknown> | undefined
  try {
    const text = await c.req.text()
    if (text) verbData = JSON.parse(text)
  } catch {
    // No body or invalid JSON — proceed with no data
  }

  const result = await stub.executeVerb(type, id, verb, verbData)
  return c.json({ success: result.success, data: result.data, error: result.error, meta: result.meta }, result.status as 200 | 400 | 403 | 404 | 500)
})

export default app
