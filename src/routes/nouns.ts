/**
 * Noun management routes
 *
 * POST /nouns       — define a new noun (registers schema with verb conjugation)
 * GET  /nouns       — list all registered nouns
 * GET  /nouns/:name — get noun schema + verbs + field definitions
 */

import { Hono } from 'hono'
import type { AppEnv, ApiResponse } from '../types'

const app = new Hono<AppEnv>()

/**
 * POST /nouns — define a new noun
 *
 * Body: { name: 'Contact', definition: { name: 'string!', email: 'string?#', ... } }
 */
app.post('/', async (c) => {
  const body = await c.req.json()
  const tenant = c.get('tenant')
  const doId = c.env.OBJECTS.idFromName(tenant)
  const stub = c.env.OBJECTS.get(doId)

  const res = await stub.fetch(
    new Request('https://do/define', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Tenant': tenant },
      body: JSON.stringify(body),
    }),
  )

  const result = (await res.json()) as ApiResponse
  return c.json(result, res.status as 200 | 201 | 400 | 500)
})

/**
 * GET /nouns — list all registered nouns
 */
app.get('/', async (c) => {
  const tenant = c.get('tenant')
  const doId = c.env.OBJECTS.idFromName(tenant)
  const stub = c.env.OBJECTS.get(doId)

  const res = await stub.fetch(new Request('https://do/nouns'))
  const result = (await res.json()) as ApiResponse
  return c.json(result)
})

/**
 * GET /nouns/:name — get noun schema with verb conjugations and fields
 */
app.get('/:name', async (c) => {
  const name = c.req.param('name')
  const tenant = c.get('tenant')
  const doId = c.env.OBJECTS.idFromName(tenant)
  const stub = c.env.OBJECTS.get(doId)

  const res = await stub.fetch(new Request(`https://do/nouns/${name}`))
  const result = (await res.json()) as ApiResponse
  return c.json(result, res.status as 200 | 404)
})

export default app
