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
 * POST /entities/:type — create a new entity
 */
app.post('/:type', async (c) => {
  const type = c.req.param('type')
  const body = await c.req.text()

  const res = await forward(c, `/entities/${type}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body,
  })

  const result = (await res.json()) as ApiResponse
  return c.json(result, res.status as 200 | 201 | 400 | 403 | 500)
})

/**
 * GET /entities/:type — list/find entities
 *
 * Query params: filter (JSON), limit, offset, sort (JSON)
 */
app.get('/:type', async (c) => {
  const type = c.req.param('type')
  const url = new URL(c.req.url)
  const qs = url.search

  const res = await forward(c, `/entities/${type}${qs}`)

  const result = (await res.json()) as ApiResponse
  return c.json(result, res.status as 200 | 400)
})

/**
 * GET /entities/:type/:id — get entity by ID
 */
app.get('/:type/:id', async (c) => {
  const type = c.req.param('type')
  const id = c.req.param('id')

  const res = await forward(c, `/entities/${type}/${id}`)

  const result = (await res.json()) as ApiResponse
  const etag = res.headers.get('ETag')
  if (etag) c.header('ETag', etag)
  return c.json(result, res.status as 200 | 404)
})

/**
 * PUT /entities/:type/:id — update entity
 */
app.put('/:type/:id', async (c) => {
  const type = c.req.param('type')
  const id = c.req.param('id')
  const body = await c.req.text()

  const headers: Record<string, string> = { 'Content-Type': 'application/json' }
  const ifMatch = c.req.header('If-Match')
  if (ifMatch) headers['If-Match'] = ifMatch

  const res = await forward(c, `/entities/${type}/${id}`, {
    method: 'PUT',
    headers,
    body,
  })

  const result = (await res.json()) as ApiResponse
  const etag = res.headers.get('ETag')
  if (etag) c.header('ETag', etag)
  return c.json(result, res.status as 200 | 404 | 409 | 500)
})

/**
 * PATCH /entities/:type/:id — partial update (alias for PUT)
 */
app.patch('/:type/:id', async (c) => {
  const type = c.req.param('type')
  const id = c.req.param('id')
  const body = await c.req.text()

  const headers: Record<string, string> = { 'Content-Type': 'application/json' }
  const ifMatch = c.req.header('If-Match')
  if (ifMatch) headers['If-Match'] = ifMatch

  const res = await forward(c, `/entities/${type}/${id}`, {
    method: 'PUT',
    headers,
    body,
  })

  const result = (await res.json()) as ApiResponse
  const etag = res.headers.get('ETag')
  if (etag) c.header('ETag', etag)
  return c.json(result, res.status as 200 | 404 | 409 | 500)
})

/**
 * DELETE /entities/:type/:id — soft delete entity
 */
app.delete('/:type/:id', async (c) => {
  const type = c.req.param('type')
  const id = c.req.param('id')

  const res = await forward(c, `/entities/${type}/${id}`, { method: 'DELETE' })

  const result = (await res.json()) as ApiResponse
  return c.json(result, res.status as 200 | 403 | 404)
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

  // Special case: "hooks" is not a verb — route to hook registration
  if (verb === 'hooks') {
    return handleHookRegistration(c, type)
  }

  const body = await c.req.text()

  const res = await forward(c, `/entities/${type}/${id}/${verb}`, {
    method: 'POST',
    headers: body ? { 'Content-Type': 'application/json' } : {},
    body: body || undefined,
  })

  const result = (await res.json()) as ApiResponse
  return c.json(result, res.status as 200 | 400 | 403 | 404 | 500)
})

/**
 * POST /entities/:type/hooks — register a hook
 */
async function handleHookRegistration(
  c: { req: { text: () => Promise<string> }; env: AppEnv['Bindings']; get: (key: 'tenant') => string; json: (data: unknown, status?: number) => Response },
  type: string,
): Promise<Response> {
  const body = await c.req.text()

  const res = await forward(c as Parameters<typeof forward>[0], `/entities/${type}/hooks`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body,
  })

  const result = (await res.json()) as ApiResponse
  return c.json(result, res.status as 200 | 201 | 400)
}

export default app
