/**
 * Verb routes — verb conjugation and discovery
 *
 * GET  /verbs              — list all verbs across all nouns (with conjugations)
 * GET  /verbs/:verb        — get verb details (which nouns have it, conjugation forms)
 * POST /verbs/conjugate    — given a verb, return all conjugation forms
 */

import { Hono } from 'hono'
import type { AppEnv } from '../types'
import { getStub } from '../lib/tenant'

const app = new Hono<AppEnv>()

/**
 * GET /verbs — list all verbs across all nouns
 *
 * Returns: { create: { conjugation: {...}, nouns: ['Contact', 'Deal'] }, ... }
 */
app.get('/', async (c) => {
  const stub = getStub(c)
  const result = await stub.listVerbs()
  return c.json(result)
})

/**
 * POST /verbs/conjugate — conjugate any verb
 *
 * Body: { verb: 'qualify' }
 * Returns: { action: 'qualify', activity: 'qualifying', event: 'qualified', ... }
 *
 * NOTE: This route must come before /:verb to avoid matching 'conjugate' as a verb name.
 */
app.post('/conjugate', async (c) => {
  const body = await c.req.json()
  const stub = getStub(c)

  const result = await stub.conjugate(body)
  return c.json({ success: result.success, data: result.data, error: result.error }, result.status as 200 | 400)
})

/**
 * GET /verbs/:verb — get verb details
 *
 * Returns which nouns have this verb and its conjugation forms.
 * Also searches by activity form (qualifying) and event form (qualified).
 */
app.get('/:verb', async (c) => {
  const verb = c.req.param('verb')
  const stub = getStub(c)

  const result = await stub.getVerb(verb)
  return c.json({ success: result.success, data: result.data, error: result.error }, result.status as 200 | 404)
})

export default app
