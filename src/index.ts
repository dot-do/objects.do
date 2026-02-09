/**
 * objects.do Worker
 *
 * Managed Digital Objects service — the hosted runtime for Noun() entities
 * with verb conjugation, event sourcing, and schema-driven APIs.
 *
 * Multi-tenancy: Each tenant gets a dedicated Durable Object with complete
 * data isolation. Tenant is resolved from:
 *   - X-Tenant header
 *   - /~:tenant path prefix (e.g., /~acme/entities/Contact)
 *   - Subdomain context (journey, system, industry)
 *
 * API Endpoints:
 *   POST /nouns                            - Define a noun (entity type + verbs)
 *   GET  /nouns                            - List all registered nouns
 *   GET  /nouns/:name                      - Get noun schema + conjugations
 *
 *   GET  /verbs                            - List all verbs across nouns
 *   GET  /verbs/:verb                      - Get verb details + which nouns use it
 *   POST /verbs/conjugate                  - Conjugate any verb
 *
 *   POST /entities/:type                   - Create entity
 *   GET  /entities/:type                   - List/find entities (filter, sort, limit)
 *   GET  /entities/:type/:id               - Get entity by ID
 *   PUT  /entities/:type/:id               - Update entity (optimistic locking)
 *   DELETE /entities/:type/:id             - Soft delete entity
 *   POST /entities/:type/:id/:verb         - Execute verb (the verb IS the endpoint)
 *   POST /entities/:type/hooks             - Register hook (code-as-data)
 *
 *   GET  /events                           - Query immutable event log
 *
 *   GET  /schema                           - Full schema (nouns, fields, verbs)
 *   GET  /schema/graph                     - Relationship graph visualization
 *   GET  /schema/openapi                   - OpenAPI 3.1 spec from nouns
 *
 *   POST   /tenants                        - Create/provision a new tenant
 *   GET    /tenants/:id                    - Get tenant info
 *   GET    /tenants/:id/stats              - Tenant statistics
 *   DELETE /tenants/:id                    - Deactivate tenant (soft delete)
 *
 *   GET  /health                           - Health check
 */

import { Hono } from 'hono'
import { cors } from 'hono/cors'
import { logger } from 'hono/logger'
import type { AppEnv, ApiResponse } from './types'

// Route modules
import nounRoutes from './routes/nouns'
import verbRoutes from './routes/verbs'
import entityRoutes from './routes/entities'
import schemaRoutes from './routes/schema'
import eventRoutes from './routes/events'
import subscriptionRoutes from './routes/subscriptions'
import tenantRoutes from './routes/tenants'
import integrationRoutes from './routes/integrations'

// Middleware
import { tenantMiddleware, extractTenantFromPath, stripTenantPrefix } from './lib/tenant'

// Durable Object export
export { ObjectsDO } from './do/objects-do'

// =============================================================================
// Main Application
// =============================================================================

const app = new Hono<AppEnv>()

// =============================================================================
// Middleware
// =============================================================================

// CORS
app.use(
  '*',
  cors({
    origin: (origin) => {
      if (!origin) return origin
      if (origin.endsWith('.do') || origin === 'https://objects.do') return origin
      if (origin.endsWith('.headless.ly') || origin === 'https://headless.ly') return origin
      if (origin.startsWith('http://localhost:') || origin.startsWith('http://127.0.0.1:')) return origin
      return null
    },
    allowMethods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
    allowHeaders: ['Content-Type', 'Authorization', 'X-API-Key', 'X-Tenant', 'If-Match'],
    exposeHeaders: ['Content-Length', 'ETag', 'X-Request-Id'],
    credentials: true,
  }),
)

// Logger (development only)
app.use('*', async (c, next) => {
  if (c.env.ENVIRONMENT === 'development') {
    return logger()(c, next)
  }
  return next()
})

// Request ID
app.use('*', async (c, next) => {
  const requestId = crypto.randomUUID()
  c.res.headers.set('X-Request-Id', requestId)
  return next()
})

// Tenant resolution — extracts tenant from X-Tenant header or /~:tenant path,
// resolves subdomain context (journey/system/industry), returns 400 if no
// tenant is provided on routes that require it
app.use('*', tenantMiddleware())

// =============================================================================
// Health Check (tenant-exempt)
// =============================================================================

app.get('/', (c) => {
  return c.json({
    name: 'objects.do',
    version: '0.0.1',
    description: 'Managed Digital Objects — verb conjugation, event sourcing, schema-driven APIs',
    status: 'ok',
    docs: 'https://objects.do/schema/openapi',
    endpoints: {
      nouns: '/nouns',
      verbs: '/verbs',
      entities: '/entities/:type',
      events: '/events',
      schema: '/schema',
      tenants: '/tenants',
      integrations: '/integrations/hooks',
      dispatchLog: '/integrations/dispatch-log',
    },
  })
})

app.get('/health', (c) => {
  return c.json({ status: 'ok', service: 'objects.do', timestamp: new Date().toISOString() })
})

// =============================================================================
// Tenant Management Routes (partially tenant-exempt)
// =============================================================================

app.route('/tenants', tenantRoutes)

// =============================================================================
// API Routes (all require tenant)
// =============================================================================

// Noun management (define, list, get)
app.route('/nouns', nounRoutes)

// Verb conjugation API
app.route('/verbs', verbRoutes)

// Entity CRUD via verb-based API
app.route('/entities', entityRoutes)

// Schema discovery
app.route('/schema', schemaRoutes)

// Event log
app.route('/events', eventRoutes)

// Subscriptions
app.route('/subscriptions', subscriptionRoutes)

// Integration hooks and dispatch log
app.route('/integrations', integrationRoutes)

// =============================================================================
// Error Handling
// =============================================================================

app.onError((err, c) => {
  console.error('[objects.do] Unhandled error:', err)

  return c.json<ApiResponse>(
    {
      success: false,
      error: c.env.ENVIRONMENT === 'development' ? err.message : 'Internal server error',
    },
    500,
  )
})

app.notFound((c) => {
  return c.json<ApiResponse>({ success: false, error: 'Not found' }, 404)
})

// =============================================================================
// Export — wraps the Hono app to rewrite /~:tenant/ path prefixes
//
// When a request arrives at /~acme/entities/Contact, the wrapper:
// 1. Extracts "acme" as the tenant
// 2. Strips the prefix → /entities/Contact
// 3. Injects X-Tenant: acme header (if not already set)
// 4. Forwards the rewritten request to the Hono router
//
// This ensures Hono's route matching works correctly regardless of whether
// the tenant is provided via path prefix or X-Tenant header.
// =============================================================================

export default {
  fetch(request: Request, env: AppEnv['Bindings'], ctx: ExecutionContext): Response | Promise<Response> {
    const url = new URL(request.url)
    const tenantFromPath = extractTenantFromPath(url.pathname)

    if (tenantFromPath) {
      // Strip the /~:tenant prefix so Hono routes match correctly.
      // Preserve the tenant ID by injecting it as X-Tenant header
      // (the tenant middleware will pick it up from the header).
      const strippedPath = stripTenantPrefix(url.pathname)
      const rewrittenUrl = new URL(strippedPath + url.search, url.origin)
      const headers = new Headers(request.headers)
      // Only set X-Tenant if not already present (header takes precedence)
      if (!headers.has('X-Tenant')) {
        headers.set('X-Tenant', tenantFromPath)
      }
      const rewritten = new Request(rewrittenUrl.toString(), {
        method: request.method,
        headers,
        body: request.body,
      })
      return app.fetch(rewritten, env, ctx)
    }

    return app.fetch(request, env, ctx)
  },
}
