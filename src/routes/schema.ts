/**
 * Schema discovery routes
 *
 * GET /schema         — full schema (all nouns, fields, relationships, verbs)
 * GET /schema/graph   — relationship graph visualization data
 * GET /schema/openapi — OpenAPI 3.1 spec generated from registered nouns
 */

import { Hono } from 'hono'
import type { AppEnv } from '../types'
import { getStub } from '../lib/tenant'

const app = new Hono<AppEnv>()

/**
 * GET /schema — full schema
 */
app.get('/', async (c) => {
  const stub = getStub(c)
  const result = await stub.fullSchema()
  return c.json(result)
})

/**
 * GET /schema/graph — relationship graph visualization data
 *
 * Returns { nodes: [...], edges: [...] } for rendering with d3, cytoscape, etc.
 */
app.get('/graph', async (c) => {
  const stub = getStub(c)
  const result = await stub.schemaGraph()
  return c.json(result)
})

/**
 * GET /schema/openapi — OpenAPI 3.1 spec
 *
 * Delegates to the DO's openAPISpec() method which generates the spec
 * from currently registered nouns.
 */
app.get('/openapi', async (c) => {
  const stub = getStub(c)
  const spec = await stub.openAPISpec()
  return c.json(spec)
})

export default app
