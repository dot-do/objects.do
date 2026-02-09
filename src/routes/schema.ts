/**
 * Schema discovery routes
 *
 * GET /schema         — full schema (all nouns, fields, relationships, verbs)
 * GET /schema/graph   — relationship graph visualization data
 * GET /schema/openapi — OpenAPI 3.1 spec generated from registered nouns
 */

import { Hono } from 'hono'
import type { AppEnv, StoredNounSchema } from '../types'
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
 * Generates OpenAPI from currently registered nouns.
 */
app.get('/openapi', async (c) => {
  const stub = getStub(c)

  // Fetch all nouns to generate the spec
  const result = await stub.listNouns()

  if (!result.success) {
    return c.json(result)
  }

  const spec = generateOpenAPISpec(result.data)
  return c.json(spec)
})

/**
 * Generate OpenAPI 3.1 spec from noun schemas
 */
function generateOpenAPISpec(nouns: StoredNounSchema[]): Record<string, unknown> {
  const paths: Record<string, unknown> = {}
  const schemas: Record<string, unknown> = {}

  for (const noun of nouns) {
    const typeLower = noun.slug
    const typeName = noun.name

    // Generate JSON Schema for the noun
    const properties: Record<string, unknown> = {
      $id: { type: 'string', description: `Unique ID (format: ${typeLower}_{sqid})` },
      $type: { type: 'string', const: typeName },
      $context: { type: 'string' },
      $version: { type: 'integer' },
      $createdAt: { type: 'string', format: 'date-time' },
      $updatedAt: { type: 'string', format: 'date-time' },
    }

    for (const [fieldName, field] of Object.entries(noun.fields)) {
      if (field.kind === 'enum' && field.enumValues) {
        properties[fieldName] = { type: 'string', enum: field.enumValues }
      } else if (field.kind === 'field') {
        const typeMap: Record<string, string> = {
          string: 'string',
          number: 'number',
          int: 'integer',
          float: 'number',
          boolean: 'boolean',
          date: 'string',
          datetime: 'string',
          json: 'object',
          url: 'string',
          email: 'string',
          text: 'string',
          markdown: 'string',
        }
        properties[fieldName] = { type: typeMap[field.type ?? 'string'] ?? 'string' }
      }
    }

    schemas[typeName] = {
      type: 'object',
      properties,
      required: ['$id', '$type'],
    }

    // CRUD paths
    paths[`/entities/${typeName}`] = {
      get: {
        summary: `List ${noun.plural}`,
        tags: [typeName],
        parameters: [
          { name: 'filter', in: 'query', schema: { type: 'string' }, description: 'JSON filter object' },
          { name: 'limit', in: 'query', schema: { type: 'integer', default: 100 } },
          { name: 'offset', in: 'query', schema: { type: 'integer', default: 0 } },
          { name: 'sort', in: 'query', schema: { type: 'string' }, description: 'JSON sort object' },
        ],
        responses: { '200': { description: `List of ${noun.plural}` } },
      },
      post: {
        summary: `Create a ${noun.singular}`,
        tags: [typeName],
        requestBody: { content: { 'application/json': { schema: { $ref: `#/components/schemas/${typeName}` } } } },
        responses: { '201': { description: `${typeName} created` } },
      },
    }

    paths[`/entities/${typeName}/{id}`] = {
      get: {
        summary: `Get a ${noun.singular} by ID`,
        tags: [typeName],
        parameters: [{ name: 'id', in: 'path', required: true, schema: { type: 'string' } }],
        responses: { '200': { description: `The ${noun.singular}` }, '404': { description: 'Not found' } },
      },
      put: {
        summary: `Update a ${noun.singular}`,
        tags: [typeName],
        parameters: [{ name: 'id', in: 'path', required: true, schema: { type: 'string' } }],
        requestBody: { content: { 'application/json': { schema: { type: 'object' } } } },
        responses: { '200': { description: `${typeName} updated` } },
      },
      delete: {
        summary: `Delete a ${noun.singular}`,
        tags: [typeName],
        parameters: [{ name: 'id', in: 'path', required: true, schema: { type: 'string' } }],
        responses: { '200': { description: `${typeName} deleted` } },
      },
    }

    // Verb-specific paths
    for (const [verbName, conj] of Object.entries(noun.verbs)) {
      if (['create', 'update', 'delete'].includes(verbName)) continue

      paths[`/entities/${typeName}/{id}/${verbName}`] = {
        post: {
          summary: `${conj.action} a ${noun.singular} (${conj.activity} -> ${conj.event})`,
          tags: [typeName],
          description: `Execute the '${verbName}' verb. Emits ${typeName}.${conj.event} event.`,
          parameters: [{ name: 'id', in: 'path', required: true, schema: { type: 'string' } }],
          requestBody: { content: { 'application/json': { schema: { type: 'object' } } }, required: false },
          responses: { '200': { description: `${typeName} ${conj.event}` } },
        },
      }
    }
  }

  return {
    openapi: '3.1.0',
    info: {
      title: 'objects.do API',
      version: '0.0.1',
      description: 'Managed Digital Objects with verb conjugation — the runtime for Noun() entities',
    },
    servers: [{ url: 'https://objects.do' }],
    paths,
    components: { schemas },
  }
}

export default app
