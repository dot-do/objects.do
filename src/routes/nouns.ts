/**
 * Noun management routes
 *
 * POST /nouns       — define a new noun (registers schema with verb conjugation)
 * GET  /nouns       — list all registered nouns
 * GET  /nouns/:name — get noun schema + verbs + field definitions
 */

import { Hono } from 'hono'
import type { AppEnv } from '../types'
import { getStub } from '../lib/tenant'

const app = new Hono<AppEnv>()

/**
 * POST /nouns — define a new noun
 *
 * Body: { name: 'Contact', definition: { name: 'string!', email: 'string?#', ... } }
 */
app.post('/', async (c) => {
  const body = await c.req.json()
  const stub = getStub(c)

  const result = await stub.defineNoun(body)
  return c.json({ success: result.success, data: result.data, error: result.error }, result.status as 200 | 201 | 400 | 500)
})

/**
 * GET /nouns — list all registered nouns
 */
app.get('/', async (c) => {
  const stub = getStub(c)
  const result = await stub.listNouns()
  return c.json(result)
})

/**
 * GET /nouns/:name — get noun schema with verb conjugations and fields
 */
app.get('/:name', async (c) => {
  const name = c.req.param('name')
  const stub = getStub(c)

  const result = await stub.getNounSchema(name)
  return c.json({ success: result.success, data: result.data, error: result.error }, result.status as 200 | 404)
})

export default app
