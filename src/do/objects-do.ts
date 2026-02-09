/**
 * ObjectsDO — Durable Object for managing Digital Objects per tenant
 *
 * Architecture:
 * - SQLite stores nouns (schemas), entities (data), events (audit log),
 *   relationships (graph edges), hooks (code-as-data), and subscriptions
 * - Every verb execution emits a full NounEvent to the immutable event log
 *   with conjugation, before/after state, and monotonic sequence
 * - Events are dispatched to registered subscriptions (webhook/websocket/code)
 * - Entities use {type}_{sqid} IDs
 * - Soft-delete: entities are marked $deletedAt, never physically removed
 * - Hooks are stored as registrations but runtime code execution is disabled for security
 * - Time travel: entity state can be reconstructed at any version or timestamp
 * - CDC: Server-Sent Events stream for external consumers
 */

import { DurableObject } from 'cloudflare:workers'
import { parseNounDefinition } from '../lib/parse'
import {
  BUILTIN_HOOKS,
  dispatchIntegrationHooks,
  findMatchingHooks,
  type IntegrationHook,
  type DispatchPayload,
  type DispatchResult,
  type ServiceBindings,
  type ServiceName,
} from '../lib/integration-dispatch'
import type { StoredNounSchema, NounInstance, VerbEvent, VerbConjugation, Hook } from '../types'

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface FullEvent {
  $id: string
  $type: string
  entityType: string
  entityId: string
  verb: string
  conjugation: { action: string; activity: string; event: string }
  data: Record<string, unknown> | null
  before: Record<string, unknown> | null
  after: Record<string, unknown> | null
  sequence: number
  timestamp: string
}

interface StoredSubscription {
  id: string
  pattern: string
  mode: 'webhook' | 'websocket'
  endpoint: string
  secret: string | null
  active: number
  created_at: string
}

// ---------------------------------------------------------------------------
// ID Generation
// ---------------------------------------------------------------------------

const SQID_CHARS = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'

function generateSqid(length = 10): string {
  let result = ''
  for (let i = 0; i < length; i++) {
    result += SQID_CHARS[Math.floor(Math.random() * SQID_CHARS.length)]
  }
  return result
}

function generateEntityId(type: string): string {
  return `${type.toLowerCase()}_${generateSqid()}`
}

function generateEventId(): string {
  return `evt_${generateSqid(12)}`
}

function generateSubscriptionId(): string {
  return `sub_${generateSqid(12)}`
}

// ---------------------------------------------------------------------------
// Verb Conjugation Helpers
// ---------------------------------------------------------------------------

/**
 * Derive verb conjugation forms from a verb string.
 * For CRUD verbs we use known forms; for custom verbs we apply simple rules.
 */
function conjugateVerb(verb: string): { action: string; activity: string; event: string } {
  const known: Record<string, { action: string; activity: string; event: string }> = {
    create: { action: 'create', activity: 'creating', event: 'created' },
    update: { action: 'update', activity: 'updating', event: 'updated' },
    delete: { action: 'delete', activity: 'deleting', event: 'deleted' },
  }

  if (known[verb]) return known[verb]

  // Simple conjugation for custom verbs
  const action = verb
  let activity: string
  let event: string

  if (verb.endsWith('e')) {
    activity = verb.slice(0, -1) + 'ing'
    event = verb + 'd'
  } else if (verb.endsWith('y')) {
    activity = verb + 'ing'
    event = verb.slice(0, -1) + 'ied'
  } else {
    activity = verb + 'ing'
    event = verb + 'ed'
  }

  return { action, activity, event }
}

// ---------------------------------------------------------------------------
// Durable Object
// ---------------------------------------------------------------------------

export class ObjectsDO extends DurableObject<Cloudflare.Env> {
  private sql: SqlStorage

  /** In-memory cache of noun schemas (hydrated from SQLite on first access) */
  private nounCache: Map<string, StoredNounSchema> | null = null

  constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
    super(ctx, env)
    this.sql = ctx.storage.sql
    this.initSchema()
  }

  // =========================================================================
  // Schema initialization
  // =========================================================================

  private initSchema(): void {
    // Tenant metadata table — stores tenant ID, name, plan, status
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS tenant_meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      )
    `)

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS nouns (
        name TEXT PRIMARY KEY,
        schema TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      )
    `)

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS entities (
        id TEXT PRIMARY KEY,
        type TEXT NOT NULL,
        data TEXT NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        deleted_at TEXT
      )
    `)

    this.sql.exec(`CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(type)`)
    this.sql.exec(`CREATE INDEX IF NOT EXISTS idx_entities_type_deleted ON entities(type, deleted_at)`)

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS events (
        id TEXT PRIMARY KEY,
        type TEXT NOT NULL,
        entity_type TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        verb TEXT NOT NULL,
        conjugation_action TEXT NOT NULL DEFAULT '',
        conjugation_activity TEXT NOT NULL DEFAULT '',
        conjugation_event TEXT NOT NULL DEFAULT '',
        data TEXT,
        before_state TEXT,
        after_state TEXT,
        sequence INTEGER NOT NULL DEFAULT 0,
        timestamp TEXT NOT NULL DEFAULT (datetime('now'))
      )
    `)

    this.sql.exec(`CREATE INDEX IF NOT EXISTS idx_events_entity ON events(entity_type, entity_id)`)
    this.sql.exec(`CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp)`)
    this.sql.exec(`CREATE INDEX IF NOT EXISTS idx_events_verb ON events(verb)`)
    this.sql.exec(`CREATE INDEX IF NOT EXISTS idx_events_sequence ON events(entity_type, entity_id, sequence)`)

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS relationships (
        subject_id TEXT NOT NULL,
        predicate TEXT NOT NULL,
        object_id TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (subject_id, predicate, object_id)
      )
    `)

    this.sql.exec(`CREATE INDEX IF NOT EXISTS idx_rels_object ON relationships(object_id, predicate)`)

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS hooks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        noun TEXT NOT NULL,
        verb TEXT NOT NULL,
        phase TEXT NOT NULL CHECK(phase IN ('before', 'after')),
        code TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      )
    `)

    this.sql.exec(`CREATE INDEX IF NOT EXISTS idx_hooks_lookup ON hooks(noun, verb, phase)`)

    // Subscriptions table for webhook/websocket event delivery
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS subscriptions (
        id TEXT PRIMARY KEY,
        pattern TEXT NOT NULL,
        mode TEXT NOT NULL CHECK(mode IN ('webhook', 'websocket')),
        endpoint TEXT NOT NULL,
        secret TEXT,
        active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      )
    `)

    this.sql.exec(`CREATE INDEX IF NOT EXISTS idx_subs_pattern ON subscriptions(pattern)`)
    this.sql.exec(`CREATE INDEX IF NOT EXISTS idx_subs_active ON subscriptions(active)`)

    // Integration hooks — per-tenant, per-verb integration routing to .do services
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS integration_hooks (
        id TEXT PRIMARY KEY,
        entity_type TEXT NOT NULL,
        verb TEXT NOT NULL,
        service TEXT NOT NULL,
        method TEXT NOT NULL,
        config TEXT,
        active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      )
    `)

    this.sql.exec(`CREATE INDEX IF NOT EXISTS idx_ihooks_lookup ON integration_hooks(entity_type, verb, active)`)

    // Dispatch log — audit trail for integration dispatches
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS dispatch_log (
        id TEXT PRIMARY KEY,
        event_id TEXT NOT NULL,
        hook_id TEXT NOT NULL,
        service TEXT NOT NULL,
        method TEXT NOT NULL,
        status TEXT NOT NULL,
        status_code INTEGER,
        error TEXT,
        duration_ms INTEGER NOT NULL,
        timestamp TEXT NOT NULL DEFAULT (datetime('now'))
      )
    `)

    this.sql.exec(`CREATE INDEX IF NOT EXISTS idx_dispatch_event ON dispatch_log(event_id)`)
    this.sql.exec(`CREATE INDEX IF NOT EXISTS idx_dispatch_timestamp ON dispatch_log(timestamp)`)

    // Migration: add new columns to existing events table if they don't exist.
    // SQLite doesn't have IF NOT EXISTS for ALTER TABLE, so we catch errors.
    try {
      this.sql.exec('SELECT conjugation_action FROM events LIMIT 0')
    } catch {
      try {
        this.sql.exec("ALTER TABLE events ADD COLUMN conjugation_action TEXT NOT NULL DEFAULT ''")
        this.sql.exec("ALTER TABLE events ADD COLUMN conjugation_activity TEXT NOT NULL DEFAULT ''")
        this.sql.exec("ALTER TABLE events ADD COLUMN conjugation_event TEXT NOT NULL DEFAULT ''")
        this.sql.exec('ALTER TABLE events ADD COLUMN before_state TEXT')
        this.sql.exec('ALTER TABLE events ADD COLUMN after_state TEXT')
        this.sql.exec('ALTER TABLE events ADD COLUMN sequence INTEGER NOT NULL DEFAULT 0')
      } catch {
        // Columns already exist or table was just created with them
      }
    }
  }

  // =========================================================================
  // Noun cache
  // =========================================================================

  private loadNouns(): Map<string, StoredNounSchema> {
    if (this.nounCache) return this.nounCache
    this.nounCache = new Map()
    const rows = this.sql.exec('SELECT name, schema FROM nouns').toArray()
    for (const row of rows) {
      const schema = JSON.parse(row.schema as string) as StoredNounSchema
      this.nounCache.set(row.name as string, schema)
    }
    return this.nounCache
  }

  private getNoun(name: string): StoredNounSchema | undefined {
    return this.loadNouns().get(name)
  }

  // =========================================================================
  // Request handler
  // =========================================================================

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    const path = url.pathname
    const method = request.method

    try {
      // -----------------------------------------------------------------------
      // Tenant management
      // -----------------------------------------------------------------------

      if (path === '/tenant/provision' && method === 'POST') {
        return this.handleProvisionTenant(request)
      }

      if (path === '/tenant/info' && method === 'GET') {
        return this.handleTenantInfo(request)
      }

      if (path === '/tenant/stats' && method === 'GET') {
        return this.handleTenantStats(request)
      }

      if (path === '/tenant/deactivate' && method === 'DELETE') {
        return this.handleDeactivateTenant(request)
      }

      // -----------------------------------------------------------------------
      // Noun management
      // -----------------------------------------------------------------------

      if (path === '/define' && method === 'POST') {
        return this.handleDefineNoun(request)
      }

      if (path === '/nouns' && method === 'GET') {
        return this.handleListNouns()
      }

      const nounMatch = path.match(/^\/nouns\/([^/]+)$/)
      if (nounMatch && method === 'GET') {
        return this.handleGetNoun(nounMatch[1]!)
      }

      // -----------------------------------------------------------------------
      // Verb listing
      // -----------------------------------------------------------------------

      if (path === '/verbs' && method === 'GET') {
        return this.handleListVerbs()
      }

      const verbDetailMatch = path.match(/^\/verbs\/([^/]+)$/)
      if (verbDetailMatch && method === 'GET') {
        return this.handleGetVerb(verbDetailMatch[1]!)
      }

      if (path === '/verbs/conjugate' && method === 'POST') {
        return this.handleConjugate(request)
      }

      // -----------------------------------------------------------------------
      // Entity CRUD
      // -----------------------------------------------------------------------

      // POST /entities/:type — create
      const createMatch = path.match(/^\/entities\/([^/]+)$/)
      if (createMatch && method === 'POST') {
        return this.handleCreateEntity(createMatch[1]!, request)
      }

      // GET /entities/:type — list/find
      const listMatch = path.match(/^\/entities\/([^/]+)$/)
      if (listMatch && method === 'GET') {
        return this.handleListEntities(listMatch[1]!, url.searchParams)
      }

      // GET /entities/:type/:id/history — entity event history (before generic get)
      const historyMatch = path.match(/^\/entities\/([^/]+)\/([^/]+)\/history$/)
      if (historyMatch && method === 'GET') {
        return this.handleEntityHistory(historyMatch[1]!, historyMatch[2]!)
      }

      // GET /entities/:type/:id/diff — diff between versions (before generic get)
      const diffMatch = path.match(/^\/entities\/([^/]+)\/([^/]+)\/diff$/)
      if (diffMatch && method === 'GET') {
        return this.handleEntityDiff(diffMatch[1]!, diffMatch[2]!, url.searchParams)
      }

      // GET /entities/:type/:id — get by ID (supports asOf and atVersion query params)
      const getMatch = path.match(/^\/entities\/([^/]+)\/([^/]+)$/)
      if (getMatch && method === 'GET') {
        const asOf = url.searchParams.get('asOf')
        const atVersion = url.searchParams.get('atVersion')
        if (asOf || atVersion) {
          return this.handleTimeTravelGet(getMatch[1]!, getMatch[2]!, url.searchParams)
        }
        return this.handleGetEntity(getMatch[1]!, getMatch[2]!)
      }

      // PUT /entities/:type/:id — update
      const updateMatch = path.match(/^\/entities\/([^/]+)\/([^/]+)$/)
      if (updateMatch && method === 'PUT') {
        return this.handleUpdateEntity(updateMatch[1]!, updateMatch[2]!, request)
      }

      // DELETE /entities/:type/:id — soft delete
      const deleteMatch = path.match(/^\/entities\/([^/]+)\/([^/]+)$/)
      if (deleteMatch && method === 'DELETE') {
        return this.handleDeleteEntity(deleteMatch[1]!, deleteMatch[2]!)
      }

      // -----------------------------------------------------------------------
      // Verb execution
      // -----------------------------------------------------------------------

      // POST /entities/:type/:id/:verb — execute verb
      const verbExecMatch = path.match(/^\/entities\/([^/]+)\/([^/]+)\/([^/]+)$/)
      if (verbExecMatch && method === 'POST') {
        return this.handleExecuteVerb(verbExecMatch[1]!, verbExecMatch[2]!, verbExecMatch[3]!, request)
      }

      // -----------------------------------------------------------------------
      // Hooks
      // -----------------------------------------------------------------------

      // POST /entities/:type/:id/hooks — register hook
      const hookMatch = path.match(/^\/entities\/([^/]+)\/hooks$/)
      if (hookMatch && method === 'POST') {
        return this.handleRegisterHook(hookMatch[1]!, request)
      }

      // -----------------------------------------------------------------------
      // Events
      // -----------------------------------------------------------------------

      if (path === '/events' && method === 'GET') {
        return this.handleQueryEvents(url.searchParams)
      }

      // GET /events/stream — SSE stream (CDC)
      if (path === '/events/stream' && method === 'GET') {
        return this.handleEventStream(url.searchParams)
      }

      // GET /events/history/:type/:id — entity event history
      const eventHistoryMatch = path.match(/^\/events\/history\/([^/]+)\/([^/]+)$/)
      if (eventHistoryMatch && method === 'GET') {
        return this.handleEntityHistory(eventHistoryMatch[1]!, eventHistoryMatch[2]!)
      }

      // GET /events/:id — get single event
      const eventGetMatch = path.match(/^\/events\/([^/]+)$/)
      if (eventGetMatch && method === 'GET') {
        return this.handleGetEvent(eventGetMatch[1]!)
      }

      // -----------------------------------------------------------------------
      // Subscriptions
      // -----------------------------------------------------------------------

      if (path === '/subscriptions' && method === 'POST') {
        return this.handleCreateSubscription(request)
      }

      if (path === '/subscriptions' && method === 'GET') {
        return this.handleListSubscriptions()
      }

      const subDeleteMatch = path.match(/^\/subscriptions\/([^/]+)$/)
      if (subDeleteMatch && method === 'DELETE') {
        return this.handleDeleteSubscription(subDeleteMatch[1]!)
      }

      // -----------------------------------------------------------------------
      // Integration Hooks
      // -----------------------------------------------------------------------

      if (path === '/integrations/hooks' && method === 'POST') {
        return this.handleCreateIntegrationHook(request)
      }

      if (path === '/integrations/hooks' && method === 'GET') {
        return this.handleListIntegrationHooks()
      }

      const ihookDeleteMatch = path.match(/^\/integrations\/hooks\/([^/]+)$/)
      if (ihookDeleteMatch && method === 'DELETE') {
        return this.handleDeleteIntegrationHook(ihookDeleteMatch[1]!)
      }

      // GET /integrations/dispatch-log — view dispatch audit log
      if (path === '/integrations/dispatch-log' && method === 'GET') {
        return this.handleQueryDispatchLog(url.searchParams)
      }

      // -----------------------------------------------------------------------
      // Schema
      // -----------------------------------------------------------------------

      if (path === '/schema' && method === 'GET') {
        return this.handleFullSchema()
      }

      if (path === '/schema/graph' && method === 'GET') {
        return this.handleSchemaGraph()
      }

      return this.json({ success: false, error: 'Not found' }, 404)
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error'
      console.error('[ObjectsDO] Error:', message)
      return this.json({ success: false, error: message }, 500)
    }
  }

  // =========================================================================
  // Noun handlers
  // =========================================================================

  private async handleDefineNoun(request: Request): Promise<Response> {
    const body = (await request.json()) as { name: string; definition: Record<string, string | null> }

    if (!body.name || typeof body.name !== 'string') {
      return this.json({ success: false, error: 'Missing or invalid name' }, 400)
    }
    if (!body.definition || typeof body.definition !== 'object' || Array.isArray(body.definition)) {
      return this.json({ success: false, error: 'Missing or invalid definition' }, 400)
    }

    // Validate PascalCase name
    if (!/^[A-Z][a-zA-Z0-9]*$/.test(body.name)) {
      return this.json({ success: false, error: 'Noun name must be PascalCase (e.g., Contact, BlogPost)' }, 400)
    }

    const schema = parseNounDefinition(body.name, body.definition)
    const schemaJson = JSON.stringify(schema)

    // Upsert — re-defining a noun updates it
    this.sql.exec("INSERT OR REPLACE INTO nouns (name, schema, created_at) VALUES (?, ?, datetime('now'))", schema.name, schemaJson)

    // Invalidate cache
    this.nounCache = null

    return this.json({ success: true, data: schema }, 201)
  }

  private handleListNouns(): Response {
    const nouns = this.loadNouns()
    const result = Array.from(nouns.values())
    return this.json({ success: true, data: result })
  }

  private handleGetNoun(name: string): Response {
    const noun = this.getNoun(name)
    if (!noun) {
      return this.json({ success: false, error: `Noun '${name}' not found` }, 404)
    }
    return this.json({ success: true, data: noun })
  }

  // =========================================================================
  // Verb handlers
  // =========================================================================

  private handleListVerbs(): Response {
    const nouns = this.loadNouns()
    const allVerbs: Record<string, { conjugation: VerbConjugation; nouns: string[] }> = {}

    for (const [nounName, schema] of nouns) {
      for (const [verb, conj] of Object.entries(schema.verbs)) {
        if (!allVerbs[verb]) {
          allVerbs[verb] = { conjugation: conj, nouns: [] }
        }
        allVerbs[verb]!.nouns.push(nounName)
      }
    }

    return this.json({ success: true, data: allVerbs })
  }

  private handleGetVerb(verb: string): Response {
    const nouns = this.loadNouns()
    const matches: { noun: string; conjugation: VerbConjugation }[] = []

    for (const [nounName, schema] of nouns) {
      const conj = schema.verbs[verb]
      if (conj) {
        matches.push({ noun: nounName, conjugation: conj })
      }
      // Also check by activity or event form
      for (const c of Object.values(schema.verbs)) {
        if (c.activity === verb || c.event === verb) {
          matches.push({ noun: nounName, conjugation: c })
        }
      }
    }

    if (matches.length === 0) {
      return this.json({ success: false, error: `Verb '${verb}' not found on any noun` }, 404)
    }

    return this.json({ success: true, data: matches })
  }

  private async handleConjugate(request: Request): Promise<Response> {
    const { deriveVerb } = await import('../lib/linguistic')
    const body = (await request.json()) as { verb: string }

    if (!body.verb || typeof body.verb !== 'string') {
      return this.json({ success: false, error: 'Missing or invalid verb' }, 400)
    }

    const derived = deriveVerb(body.verb)
    return this.json({
      success: true,
      data: {
        action: derived.action,
        activity: derived.activity,
        event: derived.event,
        reverseBy: derived.reverseBy,
        reverseAt: derived.reverseAt,
      },
    })
  }

  // =========================================================================
  // Entity handlers
  // =========================================================================

  private async handleCreateEntity(type: string, request: Request): Promise<Response> {
    const noun = this.getNoun(type)
    if (!noun) {
      return this.json({ success: false, error: `Noun '${type}' is not defined. Define it first via POST /nouns` }, 400)
    }

    // Check that create verb is not disabled
    if (noun.disabledVerbs.includes('create')) {
      return this.json({ success: false, error: `Verb 'create' is disabled on ${type}` }, 403)
    }

    const data = (await request.json()) as Record<string, unknown>
    const id = (data.$id as string) || generateEntityId(type)
    const now = new Date().toISOString()
    const contextUrl = request.headers.get('X-Context-URL') ?? this.getTenantContextUrl(request)

    const entity: NounInstance = {
      ...data,
      $id: id,
      $type: type,
      $context: contextUrl,
      $version: 1,
      $createdAt: now,
      $updatedAt: now,
    }

    this.sql.exec(
      'INSERT INTO entities (id, type, data, version, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)',
      id,
      type,
      JSON.stringify(entity),
      1,
      now,
      now,
    )

    // Emit event with full NounEvent shape
    const event = this.logEvent(type, id, 'create', entity, null, entity, contextUrl)

    return this.json({ success: true, data: entity, meta: { eventId: event.$id } }, 201)
  }

  private handleGetEntity(type: string, id: string): Response {
    const row = this.sql.exec('SELECT data FROM entities WHERE id = ? AND type = ? AND deleted_at IS NULL', id, type).toArray()[0]

    if (!row) {
      return this.json({ success: false, error: 'Not found' }, 404)
    }

    const entity = JSON.parse(row.data as string) as NounInstance
    return this.json({ success: true, data: entity }, 200, { ETag: `"${entity.$version}"` })
  }

  private handleListEntities(type: string, params: URLSearchParams): Response {
    const limit = Math.min(parseInt(params.get('limit') || '100', 10), 1000)
    const offset = parseInt(params.get('offset') || '0', 10)
    const filterParam = params.get('filter')
    const sortParam = params.get('sort')

    let rows = this.sql
      .exec('SELECT data FROM entities WHERE type = ? AND deleted_at IS NULL ORDER BY created_at DESC LIMIT ? OFFSET ?', type, limit, offset)
      .toArray()

    let entities = rows.map((r) => JSON.parse(r.data as string) as NounInstance)

    // Apply filter (simple field equality)
    if (filterParam) {
      try {
        const filter = JSON.parse(filterParam) as Record<string, unknown>
        entities = entities.filter((e) => {
          for (const [key, value] of Object.entries(filter)) {
            if (e[key] !== value) return false
          }
          return true
        })
      } catch {
        return this.json({ success: false, error: 'Invalid filter JSON' }, 400)
      }
    }

    // Sort
    if (sortParam) {
      try {
        const sort = JSON.parse(sortParam) as Record<string, 1 | -1>
        const [field, dir] = Object.entries(sort)[0] ?? ['$createdAt', -1]
        entities.sort((a, b) => {
          const av = a[field!] as string | number
          const bv = b[field!] as string | number
          if (av < bv) return dir === 1 ? -1 : 1
          if (av > bv) return dir === 1 ? 1 : -1
          return 0
        })
      } catch {
        // Ignore invalid sort
      }
    }

    // Total count
    const countRow = this.sql.exec('SELECT COUNT(*) as cnt FROM entities WHERE type = ? AND deleted_at IS NULL', type).toArray()[0]
    const total = (countRow?.cnt as number) ?? 0

    return this.json({
      success: true,
      data: entities,
      meta: { total, limit, offset, hasMore: offset + entities.length < total },
    })
  }

  private async handleUpdateEntity(type: string, id: string, request: Request): Promise<Response> {
    const noun = this.getNoun(type)
    if (noun && noun.disabledVerbs.includes('update')) {
      return this.json({ success: false, error: `Verb 'update' is disabled on ${type}` }, 403)
    }

    const row = this.sql.exec('SELECT data, version FROM entities WHERE id = ? AND type = ? AND deleted_at IS NULL', id, type).toArray()[0]

    if (!row) {
      return this.json({ success: false, error: 'Not found' }, 404)
    }

    const existing = JSON.parse(row.data as string) as NounInstance
    const currentVersion = row.version as number
    const updates = (await request.json()) as Record<string, unknown>

    // Optimistic locking: check $version or If-Match header
    let expectedVersion: number | undefined
    if (updates.$version !== undefined) {
      expectedVersion = Number(updates.$version)
    } else {
      const ifMatch = request.headers.get('If-Match')
      if (ifMatch) {
        const parsed = parseInt(ifMatch.replace(/"/g, ''), 10)
        if (!isNaN(parsed)) expectedVersion = parsed
      }
    }

    if (expectedVersion !== undefined && expectedVersion !== currentVersion) {
      return this.json({ success: false, error: 'Version conflict', meta: { currentVersion, expectedVersion } }, 409, { ETag: `"${currentVersion}"` })
    }

    const { $version: _v, $id: _i, $type: _t, $context: _c, $createdAt: _ca, ...userUpdates } = updates
    const now = new Date().toISOString()
    const nextVersion = currentVersion + 1

    const updated: NounInstance = {
      ...existing,
      ...userUpdates,
      $id: id,
      $type: type,
      $context: existing.$context,
      $version: nextVersion,
      $createdAt: existing.$createdAt,
      $updatedAt: now,
    }

    this.sql.exec('UPDATE entities SET data = ?, version = ?, updated_at = ? WHERE id = ?', JSON.stringify(updated), nextVersion, now, id)

    const event = this.logEvent(type, id, 'update', updated, existing, updated, existing.$context)

    return this.json({ success: true, data: updated, meta: { eventId: event.$id } }, 200, { ETag: `"${nextVersion}"` })
  }

  private handleDeleteEntity(type: string, id: string): Response {
    const noun = this.getNoun(type)
    if (noun && noun.disabledVerbs.includes('delete')) {
      return this.json({ success: false, error: `Verb 'delete' is disabled on ${type}` }, 403)
    }

    const row = this.sql.exec('SELECT data FROM entities WHERE id = ? AND type = ? AND deleted_at IS NULL', id, type).toArray()[0]

    if (!row) {
      return this.json({ success: false, error: 'Not found' }, 404)
    }

    const existing = JSON.parse(row.data as string) as NounInstance
    const now = new Date().toISOString()

    // Soft delete
    this.sql.exec('UPDATE entities SET deleted_at = ?, updated_at = ? WHERE id = ?', now, now, id)

    const event = this.logEvent(type, id, 'delete', null, existing, null, existing.$context)

    return this.json({ success: true, meta: { eventId: event.$id } })
  }

  // =========================================================================
  // Verb execution
  // =========================================================================

  private async handleExecuteVerb(type: string, id: string, verb: string, request: Request): Promise<Response> {
    const noun = this.getNoun(type)
    if (!noun) {
      return this.json({ success: false, error: `Noun '${type}' is not defined` }, 400)
    }

    // Resolve verb — check action form, then activity/event forms
    const conj = noun.verbs[verb]
    if (!conj) {
      // Check if it's a known verb by activity or event form
      const verbEntry = Object.values(noun.verbs).find((v) => v.activity === verb || v.event === verb)
      if (!verbEntry) {
        return this.json({ success: false, error: `Verb '${verb}' is not defined on ${type}` }, 400)
      }
      // Don't allow calling by activity/event form — only action form
      return this.json({ success: false, error: `Use the action form '${verbEntry.action}' instead of '${verb}'` }, 400)
    }

    // Check disabled
    if (noun.disabledVerbs.includes(verb)) {
      return this.json({ success: false, error: `Verb '${verb}' is disabled on ${type}` }, 403)
    }

    // Fetch entity
    const row = this.sql.exec('SELECT data, version FROM entities WHERE id = ? AND type = ? AND deleted_at IS NULL', id, type).toArray()[0]

    if (!row) {
      return this.json({ success: false, error: 'Not found' }, 404)
    }

    const existing = JSON.parse(row.data as string) as NounInstance
    const currentVersion = row.version as number

    let verbData: Record<string, unknown> = {}
    try {
      const body = await request.text()
      if (body) verbData = JSON.parse(body)
    } catch {
      // No body or invalid JSON — proceed with empty data
    }

    // Run BEFORE hooks
    const beforeHooks = this.getHooks(type, verb, 'before')
    for (const hook of beforeHooks) {
      console.warn(`[ObjectsDO] Runtime hook execution disabled for security — hook: ${type}.${verb}:before`)
    }

    // Apply verb data as an update
    const now = new Date().toISOString()
    const nextVersion = currentVersion + 1

    const updated: NounInstance = {
      ...existing,
      ...verbData,
      $id: id,
      $type: type,
      $context: existing.$context,
      $version: nextVersion,
      $createdAt: existing.$createdAt,
      $updatedAt: now,
    }

    this.sql.exec('UPDATE entities SET data = ?, version = ?, updated_at = ? WHERE id = ?', JSON.stringify(updated), nextVersion, now, id)

    // Emit event: Contact.qualified with before/after state
    const event = this.logEvent(type, id, verb, updated, existing, updated, existing.$context)

    // Run AFTER hooks
    const afterHooks = this.getHooks(type, verb, 'after')
    for (const hook of afterHooks) {
      console.warn(`[ObjectsDO] Runtime hook execution disabled for security — hook: ${type}.${verb}:after`)
    }

    return this.json({ success: true, data: updated, meta: { event } })
  }

  // =========================================================================
  // Hooks
  // =========================================================================

  private async handleRegisterHook(type: string, request: Request): Promise<Response> {
    const noun = this.getNoun(type)
    if (!noun) {
      return this.json({ success: false, error: `Noun '${type}' is not defined` }, 400)
    }

    const body = (await request.json()) as { verb: string; phase: 'before' | 'after'; code: string }

    if (!body.verb || typeof body.verb !== 'string') {
      return this.json({ success: false, error: 'Missing or invalid verb' }, 400)
    }
    if (!body.phase || (body.phase !== 'before' && body.phase !== 'after')) {
      return this.json({ success: false, error: "phase must be 'before' or 'after'" }, 400)
    }
    if (!body.code || typeof body.code !== 'string') {
      return this.json({ success: false, error: 'Missing or invalid code' }, 400)
    }

    // Verify verb exists on noun
    if (!noun.verbs[body.verb]) {
      return this.json({ success: false, error: `Verb '${body.verb}' is not defined on ${type}` }, 400)
    }

    this.sql.exec('INSERT INTO hooks (noun, verb, phase, code) VALUES (?, ?, ?, ?)', type, body.verb, body.phase, body.code)

    return this.json({ success: true, data: { noun: type, verb: body.verb, phase: body.phase } }, 201)
  }

  private getHooks(noun: string, verb: string, phase: 'before' | 'after'): Hook[] {
    const rows = this.sql.exec('SELECT noun, verb, phase, code, created_at FROM hooks WHERE noun = ? AND verb = ? AND phase = ?', noun, verb, phase).toArray()

    return rows.map((r) => ({
      noun: r.noun as string,
      verb: r.verb as string,
      phase: r.phase as 'before' | 'after',
      code: r.code as string,
      createdAt: r.created_at as string,
    }))
  }

  // =========================================================================
  // Events
  // =========================================================================

  /**
   * Log a full NounEvent with conjugation, before/after state, and monotonic sequence.
   * Also dispatches to registered subscriptions and integration hooks.
   */
  private logEvent(
    entityType: string,
    entityId: string,
    verb: string,
    data: Record<string, unknown> | null,
    beforeState: Record<string, unknown> | null,
    afterState: Record<string, unknown> | null,
    contextUrl?: string,
  ): FullEvent {
    const id = generateEventId()
    const now = new Date().toISOString()
    const eventType = `${entityType}.${verb}`
    const conj = conjugateVerb(verb)

    // Compute next sequence for this entity
    const seqRow = this.sql.exec('SELECT MAX(sequence) as max_seq FROM events WHERE entity_type = ? AND entity_id = ?', entityType, entityId).toArray()[0]
    const currentSeq = (seqRow?.max_seq as number) ?? 0
    const nextSeq = currentSeq + 1

    const event: FullEvent = {
      $id: id,
      $type: eventType,
      entityType,
      entityId,
      verb,
      conjugation: conj,
      data,
      before: beforeState,
      after: afterState,
      sequence: nextSeq,
      timestamp: now,
    }

    this.sql.exec(
      'INSERT INTO events (id, type, entity_type, entity_id, verb, conjugation_action, conjugation_activity, conjugation_event, data, before_state, after_state, sequence, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      id,
      eventType,
      entityType,
      entityId,
      verb,
      conj.action,
      conj.activity,
      conj.event,
      data ? JSON.stringify(data) : null,
      beforeState ? JSON.stringify(beforeState) : null,
      afterState ? JSON.stringify(afterState) : null,
      nextSeq,
      now,
    )

    // Dispatch to subscriptions (fire-and-forget)
    this.dispatchToSubscriptions(event)

    // Dispatch to integration hooks (fire-and-forget)
    this.dispatchIntegrations(event, contextUrl ?? `https://headless.ly/~default`)

    return event
  }

  // =========================================================================
  // Integration hook dispatch
  // =========================================================================

  /**
   * Build the service bindings map from the environment.
   * Returns only the bindings that are available (non-null).
   */
  private getServiceBindings(): ServiceBindings {
    const bindings: ServiceBindings = {}
    const env = this.env as Record<string, unknown>
    const serviceNames: ServiceName[] = ['PAYMENTS', 'REPO', 'INTEGRATIONS', 'OAUTH', 'EVENTS']
    for (const name of serviceNames) {
      if (env[name] && typeof (env[name] as { fetch?: unknown }).fetch === 'function') {
        bindings[name] = env[name] as { fetch(request: Request): Promise<Response> }
      }
    }
    return bindings
  }

  /**
   * Load tenant-configured integration hooks from SQLite.
   */
  private loadIntegrationHooks(entityType: string, verb: string): IntegrationHook[] {
    const rows = this.sql
      .exec(
        "SELECT * FROM integration_hooks WHERE active = 1 AND (entity_type = ? OR entity_type = '*') AND (verb = ? OR verb = '*')",
        entityType,
        verb,
      )
      .toArray()

    return rows.map((r) => ({
      id: r.id as string,
      entityType: r.entity_type as string,
      verb: r.verb as string,
      service: r.service as string,
      method: r.method as string,
      config: r.config ? JSON.parse(r.config as string) : null,
      active: (r.active as number) === 1,
      createdAt: r.created_at as string,
    }))
  }

  /**
   * Dispatch an event to all matching integration hooks.
   * Fire-and-forget: errors are caught, logged, and stored in dispatch_log.
   */
  private dispatchIntegrations(event: FullEvent, contextUrl: string): void {
    const services = this.getServiceBindings()

    // If no services are bound, skip dispatch entirely
    if (Object.keys(services).length === 0) return

    const tenantHooks = this.loadIntegrationHooks(event.entityType, event.verb)

    const payload: DispatchPayload = {
      event: event.$type,
      entityType: event.entityType,
      entityId: event.entityId,
      verb: event.verb,
      conjugation: event.conjugation,
      before: event.before,
      after: event.after,
      data: event.data,
      context: contextUrl,
      timestamp: event.timestamp,
    }

    // Fire-and-forget: dispatch in background, log results
    dispatchIntegrationHooks(services, payload, tenantHooks)
      .then((results) => {
        this.logDispatchResults(event.$id, results)
      })
      .catch((err) => {
        console.error('[ObjectsDO] Integration dispatch failed:', err instanceof Error ? err.message : err)
      })
  }

  /**
   * Store dispatch results in the dispatch_log table for audit/time-travel.
   */
  private logDispatchResults(eventId: string, results: DispatchResult[]): void {
    for (const result of results) {
      const id = `dsp_${generateSqid(12)}`
      this.sql.exec(
        'INSERT INTO dispatch_log (id, event_id, hook_id, service, method, status, status_code, error, duration_ms, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        id,
        eventId,
        result.hookId,
        result.service,
        result.method,
        result.status,
        result.statusCode ?? null,
        result.error ?? null,
        result.durationMs,
        result.timestamp,
      )
    }
  }

  // =========================================================================
  // Integration Hook Management
  // =========================================================================

  /**
   * POST /integrations/hooks — register a new integration hook
   */
  private async handleCreateIntegrationHook(request: Request): Promise<Response> {
    const body = (await request.json()) as {
      entityType: string
      verb: string
      service: string
      method: string
      config?: Record<string, unknown>
    }

    if (!body.entityType || typeof body.entityType !== 'string') {
      return this.json({ success: false, error: 'Missing or invalid entityType' }, 400)
    }
    if (!body.verb || typeof body.verb !== 'string') {
      return this.json({ success: false, error: 'Missing or invalid verb' }, 400)
    }
    if (!body.service || typeof body.service !== 'string') {
      return this.json({ success: false, error: 'Missing or invalid service' }, 400)
    }

    const validServices: ServiceName[] = ['PAYMENTS', 'REPO', 'INTEGRATIONS', 'OAUTH', 'EVENTS']
    if (!validServices.includes(body.service as ServiceName)) {
      return this.json({ success: false, error: `Invalid service. Must be one of: ${validServices.join(', ')}` }, 400)
    }

    if (!body.method || typeof body.method !== 'string') {
      return this.json({ success: false, error: 'Missing or invalid method (e.g., "POST /customers/sync")' }, 400)
    }

    const id = `ihook_${generateSqid(12)}`
    const now = new Date().toISOString()

    this.sql.exec(
      'INSERT INTO integration_hooks (id, entity_type, verb, service, method, config, active, created_at) VALUES (?, ?, ?, ?, ?, ?, 1, ?)',
      id,
      body.entityType,
      body.verb,
      body.service,
      body.method,
      body.config ? JSON.stringify(body.config) : null,
      now,
    )

    return this.json(
      {
        success: true,
        data: {
          id,
          entityType: body.entityType,
          verb: body.verb,
          service: body.service,
          method: body.method,
          config: body.config ?? null,
          active: true,
          createdAt: now,
        },
      },
      201,
    )
  }

  /**
   * GET /integrations/hooks — list all integration hooks (built-in + tenant-configured)
   */
  private handleListIntegrationHooks(): Response {
    const rows = this.sql.exec('SELECT * FROM integration_hooks ORDER BY created_at DESC').toArray()

    const tenantHooks = rows.map((r) => ({
      id: r.id as string,
      entityType: r.entity_type as string,
      verb: r.verb as string,
      service: r.service as string,
      method: r.method as string,
      config: r.config ? JSON.parse(r.config as string) : null,
      active: (r.active as number) === 1,
      createdAt: r.created_at as string,
      builtin: false,
    }))

    // Include built-in hooks for reference
    const builtinList = BUILTIN_HOOKS.map((h: { entityType: string; verb: string; service: string; method: string }) => ({
      id: `builtin:${h.service}:${h.method}`,
      entityType: h.entityType,
      verb: h.verb,
      service: h.service,
      method: h.method,
      config: null,
      active: true,
      createdAt: null,
      builtin: true,
    }))

    return this.json({ success: true, data: { builtin: builtinList, tenant: tenantHooks } })
  }

  /**
   * DELETE /integrations/hooks/:id — remove an integration hook
   */
  private handleDeleteIntegrationHook(hookId: string): Response {
    if (hookId.startsWith('builtin:')) {
      return this.json({ success: false, error: 'Cannot delete built-in hooks' }, 403)
    }

    const rows = this.sql.exec('SELECT id FROM integration_hooks WHERE id = ?', hookId).toArray()

    if (rows.length === 0) {
      return this.json({ success: false, error: 'Integration hook not found' }, 404)
    }

    this.sql.exec('DELETE FROM integration_hooks WHERE id = ?', hookId)

    return this.json({ success: true })
  }

  /**
   * GET /integrations/dispatch-log — query the dispatch audit log
   *
   * Query params: eventId, service, status, limit
   */
  private handleQueryDispatchLog(params: URLSearchParams): Response {
    const eventId = params.get('eventId')
    const service = params.get('service')
    const status = params.get('status')
    const limit = Math.min(parseInt(params.get('limit') || '100', 10), 1000)

    let query = 'SELECT * FROM dispatch_log'
    const conditions: string[] = []
    const values: (string | number)[] = []

    if (eventId) {
      conditions.push('event_id = ?')
      values.push(eventId)
    }
    if (service) {
      conditions.push('service = ?')
      values.push(service)
    }
    if (status) {
      conditions.push('status = ?')
      values.push(status)
    }

    if (conditions.length > 0) {
      query += ' WHERE ' + conditions.join(' AND ')
    }

    query += ' ORDER BY timestamp DESC LIMIT ?'
    values.push(limit)

    const rows = this.sql.exec(query, ...values).toArray()

    const results = rows.map((r) => ({
      id: r.id as string,
      eventId: r.event_id as string,
      hookId: r.hook_id as string,
      service: r.service as string,
      method: r.method as string,
      status: r.status as string,
      statusCode: r.status_code as number | null,
      error: r.error as string | null,
      durationMs: r.duration_ms as number,
      timestamp: r.timestamp as string,
    }))

    return this.json({ success: true, data: results })
  }

  private handleQueryEvents(params: URLSearchParams): Response {
    const since = params.get('since')
    const type = params.get('type')
    const entityId = params.get('entityId')
    const verb = params.get('verb')
    const limit = Math.min(parseInt(params.get('limit') || '100', 10), 1000)

    let query = 'SELECT * FROM events'
    const conditions: string[] = []
    const values: (string | number)[] = []

    if (since) {
      conditions.push('timestamp > ?')
      values.push(since)
    }
    if (type) {
      conditions.push('entity_type = ?')
      values.push(type)
    }
    if (entityId) {
      conditions.push('entity_id = ?')
      values.push(entityId)
    }
    if (verb) {
      conditions.push('verb = ?')
      values.push(verb)
    }

    if (conditions.length > 0) {
      query += ' WHERE ' + conditions.join(' AND ')
    }

    query += ' ORDER BY timestamp DESC LIMIT ?'
    values.push(limit)

    const rows = this.sql.exec(query, ...values).toArray()

    const events = rows.map((r) => this.rowToFullEvent(r))

    return this.json({ success: true, data: events })
  }

  /**
   * GET /events/:id — get a single event by ID
   */
  private handleGetEvent(eventId: string): Response {
    const rows = this.sql.exec('SELECT * FROM events WHERE id = ?', eventId).toArray()

    if (rows.length === 0) {
      return this.json({ success: false, error: 'Event not found' }, 404)
    }

    return this.json({ success: true, data: this.rowToFullEvent(rows[0]) })
  }

  /**
   * GET /events/history/:type/:id — full event history for an entity
   */
  private handleEntityHistory(entityType: string, entityId: string): Response {
    const rows = this.sql
      .exec('SELECT * FROM events WHERE entity_type = ? AND entity_id = ? ORDER BY sequence ASC', entityType, entityId)
      .toArray()

    const events = rows.map((r) => this.rowToFullEvent(r))

    return this.json({ success: true, data: events })
  }

  /**
   * GET /events/stream — Server-Sent Events (SSE) stream for CDC
   *
   * Query params:
   *   since  — cursor (event ID to start after)
   *   types  — comma-separated entity types to filter
   *   verbs  — comma-separated verbs to filter
   */
  private handleEventStream(params: URLSearchParams): Response {
    const sinceId = params.get('since')
    const typesParam = params.get('types')
    const verbsParam = params.get('verbs')

    const types = typesParam ? typesParam.split(',') : null
    const verbs = verbsParam ? verbsParam.split(',') : null

    const sql = this.sql
    const encoder = new TextEncoder()

    // Fetch buffered events since cursor
    let bufferedQuery = 'SELECT * FROM events'
    const conditions: string[] = []
    const values: (string | number)[] = []

    if (sinceId) {
      // Get timestamp of the cursor event for position
      const cursorRow = sql.exec('SELECT timestamp FROM events WHERE id = ?', sinceId).toArray()[0]
      if (cursorRow) {
        conditions.push('(timestamp > ? OR (timestamp = ? AND id > ?))')
        values.push(cursorRow.timestamp as string, cursorRow.timestamp as string, sinceId)
      }
    }

    if (types) {
      const placeholders = types.map(() => '?').join(', ')
      conditions.push(`entity_type IN (${placeholders})`)
      values.push(...types)
    }

    if (verbs) {
      const placeholders = verbs.map(() => '?').join(', ')
      conditions.push(`verb IN (${placeholders})`)
      values.push(...verbs)
    }

    if (conditions.length > 0) {
      bufferedQuery += ' WHERE ' + conditions.join(' AND ')
    }

    bufferedQuery += ' ORDER BY timestamp ASC, id ASC'

    const rowToEvent = this.rowToFullEvent.bind(this)

    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        // 1. Emit all buffered events
        const rows = sql.exec(bufferedQuery, ...values).toArray()
        for (const row of rows) {
          const event = rowToEvent(row)
          const sseMsg = `id: ${event.$id}\nevent: ${event.$type}\ndata: ${JSON.stringify(event)}\n\n`
          controller.enqueue(encoder.encode(sseMsg))
        }

        // 2. Send initial heartbeat (new events will come via subsequent requests)
        controller.enqueue(encoder.encode(': heartbeat\n\n'))

        // Close stream after emitting buffered events.
        // (In a real Durable Object with WebSocket hibernation, we'd keep
        //  the connection open and push new events. Since we're using basic
        //  HTTP SSE, the client should reconnect with the last event ID.)
        controller.close()
      },
    })

    return new Response(stream, {
      status: 200,
      headers: {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
        'X-Accel-Buffering': 'no',
      },
    })
  }

  // =========================================================================
  // Time Travel
  // =========================================================================

  /**
   * GET /entities/:type/:id?asOf=...&atVersion=... — reconstruct state at a point in time
   */
  private handleTimeTravelGet(type: string, id: string, params: URLSearchParams): Response {
    const asOf = params.get('asOf')
    const atVersionStr = params.get('atVersion')

    // Fetch event history for this entity
    let eventsQuery = 'SELECT * FROM events WHERE entity_type = ? AND entity_id = ?'
    const values: (string | number)[] = [type, id]

    if (atVersionStr) {
      const atVersion = parseInt(atVersionStr, 10)
      if (isNaN(atVersion)) {
        return this.json({ success: false, error: 'Invalid atVersion parameter' }, 400)
      }
      eventsQuery += ' AND sequence <= ?'
      values.push(atVersion)
    }

    if (asOf) {
      eventsQuery += ' AND timestamp <= ?'
      values.push(asOf)
    }

    eventsQuery += ' ORDER BY sequence ASC'

    const rows = this.sql.exec(eventsQuery, ...values).toArray()

    if (rows.length === 0) {
      return this.json({ success: false, error: 'No events found for this entity at the specified point in time' }, 404)
    }

    // Replay events to reconstruct state
    const state = this.replayEvents(rows.map((r) => this.rowToFullEvent(r)))

    if (!state) {
      return this.json({ success: false, error: 'Could not reconstruct state' }, 404)
    }

    return this.json({ success: true, data: state })
  }

  /**
   * GET /entities/:type/:id/diff?from=1&to=3 — diff between versions
   */
  private handleEntityDiff(type: string, id: string, params: URLSearchParams): Response {
    const fromStr = params.get('from')
    const toStr = params.get('to')

    if (!fromStr || !toStr) {
      return this.json({ success: false, error: 'Both from and to parameters are required' }, 400)
    }

    const fromVersion = parseInt(fromStr, 10)
    const toVersion = parseInt(toStr, 10)

    if (isNaN(fromVersion) || isNaN(toVersion)) {
      return this.json({ success: false, error: 'from and to must be valid integers' }, 400)
    }

    // Fetch all events for this entity up to the 'to' version
    const allRows = this.sql
      .exec('SELECT * FROM events WHERE entity_type = ? AND entity_id = ? ORDER BY sequence ASC', type, id)
      .toArray()

    if (allRows.length === 0) {
      return this.json({ success: false, error: 'No events found for this entity' }, 404)
    }

    const allEvents = allRows.map((r) => this.rowToFullEvent(r))

    const fromEvents = allEvents.filter((e) => e.sequence <= fromVersion)
    const toEvents = allEvents.filter((e) => e.sequence <= toVersion)
    const betweenEvents = allEvents.filter((e) => e.sequence > fromVersion && e.sequence <= toVersion)

    const beforeState = this.replayEvents(fromEvents)
    const afterState = this.replayEvents(toEvents)

    // Compute field-level changes
    const changes = this.computeChanges(beforeState, afterState)

    return this.json({
      success: true,
      data: {
        before: beforeState,
        after: afterState,
        events: betweenEvents,
        changes,
      },
    })
  }

  /**
   * Replay events in order to reconstruct entity state.
   */
  private replayEvents(events: FullEvent[]): Record<string, unknown> | null {
    if (events.length === 0) return null

    let state: Record<string, unknown> | null = null

    for (const event of events) {
      const verbEvent = event.conjugation.event

      if (verbEvent === 'deleted') {
        if (state) {
          state = Object.assign({}, state, { $deleted: true, $version: event.sequence })
        }
        continue
      }

      const afterState = event.after
      if (afterState) {
        if (!state) {
          state = Object.assign({ $id: event.entityId, $type: event.entityType, $version: event.sequence }, afterState)
        } else {
          state = Object.assign({}, state, afterState, { $id: event.entityId, $type: event.entityType, $version: event.sequence })
        }
      } else if (!state) {
        state = {
          $id: event.entityId,
          $type: event.entityType,
          $version: event.sequence,
        }
      } else {
        state.$version = event.sequence
      }
    }

    return state
  }

  /**
   * Compute field-level changes between two states.
   */
  private computeChanges(
    before: Record<string, unknown> | null,
    after: Record<string, unknown> | null,
  ): Array<{ field: string; from: unknown; to: unknown }> {
    const changes: Array<{ field: string; from: unknown; to: unknown }> = []

    if (!before && !after) return changes

    const allFields = new Set<string>()
    if (before) {
      for (const key of Object.keys(before)) {
        if (!key.startsWith('$')) allFields.add(key)
      }
    }
    if (after) {
      for (const key of Object.keys(after)) {
        if (!key.startsWith('$')) allFields.add(key)
      }
    }

    for (const field of allFields) {
      const fromVal = before?.[field]
      const toVal = after?.[field]
      if (JSON.stringify(fromVal) !== JSON.stringify(toVal)) {
        changes.push({ field, from: fromVal, to: toVal })
      }
    }

    return changes
  }

  // =========================================================================
  // Subscriptions
  // =========================================================================

  /**
   * POST /subscriptions — register a webhook or websocket subscription
   */
  private async handleCreateSubscription(request: Request): Promise<Response> {
    const body = (await request.json()) as { pattern: string; mode: 'webhook' | 'websocket'; endpoint: string; secret?: string }

    if (!body.pattern || typeof body.pattern !== 'string') {
      return this.json({ success: false, error: 'Missing or invalid pattern' }, 400)
    }
    if (!body.mode || (body.mode !== 'webhook' && body.mode !== 'websocket')) {
      return this.json({ success: false, error: "mode must be 'webhook' or 'websocket'" }, 400)
    }
    if (!body.endpoint || typeof body.endpoint !== 'string') {
      return this.json({ success: false, error: 'Missing or invalid endpoint' }, 400)
    }

    const id = generateSubscriptionId()
    const now = new Date().toISOString()

    this.sql.exec(
      'INSERT INTO subscriptions (id, pattern, mode, endpoint, secret, active, created_at) VALUES (?, ?, ?, ?, ?, 1, ?)',
      id,
      body.pattern,
      body.mode,
      body.endpoint,
      body.secret ?? null,
      now,
    )

    return this.json({
      success: true,
      data: {
        id,
        pattern: body.pattern,
        mode: body.mode,
        endpoint: body.endpoint,
        active: true,
        createdAt: now,
      },
    }, 201)
  }

  /**
   * GET /subscriptions — list all subscriptions
   */
  private handleListSubscriptions(): Response {
    const rows = this.sql.exec('SELECT * FROM subscriptions ORDER BY created_at DESC').toArray()

    const subs = rows.map((r) => ({
      id: r.id as string,
      pattern: r.pattern as string,
      mode: r.mode as string,
      endpoint: r.endpoint as string,
      active: (r.active as number) === 1,
      createdAt: r.created_at as string,
    }))

    return this.json({ success: true, data: subs })
  }

  /**
   * DELETE /subscriptions/:id — remove a subscription
   */
  private handleDeleteSubscription(subId: string): Response {
    const rows = this.sql.exec('SELECT id FROM subscriptions WHERE id = ?', subId).toArray()

    if (rows.length === 0) {
      return this.json({ success: false, error: 'Subscription not found' }, 404)
    }

    this.sql.exec('DELETE FROM subscriptions WHERE id = ?', subId)

    return this.json({ success: true })
  }

  /**
   * Dispatch an event to all matching active subscriptions (fire-and-forget).
   * For webhook mode, POSTs the event to the endpoint with optional HMAC signing.
   */
  private dispatchToSubscriptions(event: FullEvent): void {
    const rows = this.sql.exec('SELECT * FROM subscriptions WHERE active = 1').toArray()

    for (const row of rows) {
      const sub: StoredSubscription = {
        id: row.id as string,
        pattern: row.pattern as string,
        mode: row.mode as 'webhook' | 'websocket',
        endpoint: row.endpoint as string,
        secret: row.secret as string | null,
        active: row.active as number,
        created_at: row.created_at as string,
      }

      if (!this.matchesPattern(sub.pattern, event.$type)) continue

      if (sub.mode === 'webhook') {
        // Fire-and-forget webhook dispatch
        this.dispatchWebhook(sub, event).catch(() => {
          // Swallow errors — don't break the main flow
        })
      }
      // websocket mode is a placeholder for future DO WebSocket integration
    }
  }

  private async dispatchWebhook(sub: StoredSubscription, event: FullEvent): Promise<void> {
    const payload = JSON.stringify(event)
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      'X-Headlessly-Event': event.$type,
      'X-Headlessly-Delivery': event.$id,
    }

    if (sub.secret) {
      const encoder = new TextEncoder()
      const key = await crypto.subtle.importKey('raw', encoder.encode(sub.secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'])
      const signature = await crypto.subtle.sign('HMAC', key, encoder.encode(payload))
      const hex = Array.from(new Uint8Array(signature))
        .map((b) => b.toString(16).padStart(2, '0'))
        .join('')
      headers['X-Headlessly-Signature'] = `sha256=${hex}`
    }

    await fetch(sub.endpoint, {
      method: 'POST',
      headers,
      body: payload,
    })
  }

  /**
   * Match an event type against a glob-style subscription pattern.
   */
  private matchesPattern(pattern: string, eventType: string): boolean {
    if (pattern === '*') return true

    const [patternEntity, patternVerb] = pattern.split('.')
    const [eventEntity, eventVerb] = eventType.split('.')

    if (patternEntity === '*') return patternVerb === eventVerb
    if (patternVerb === '*') return patternEntity === eventEntity

    return pattern === eventType
  }

  // =========================================================================
  // Event row helpers
  // =========================================================================

  private rowToFullEvent(row: Record<string, unknown>): FullEvent {
    return {
      $id: row.id as string,
      $type: row.type as string,
      entityType: row.entity_type as string,
      entityId: row.entity_id as string,
      verb: row.verb as string,
      conjugation: {
        action: (row.conjugation_action as string) || row.verb as string,
        activity: (row.conjugation_activity as string) || '',
        event: (row.conjugation_event as string) || '',
      },
      data: row.data ? JSON.parse(row.data as string) : null,
      before: row.before_state ? JSON.parse(row.before_state as string) : null,
      after: row.after_state ? JSON.parse(row.after_state as string) : null,
      sequence: (row.sequence as number) ?? 0,
      timestamp: row.timestamp as string,
    }
  }

  // =========================================================================
  // Schema
  // =========================================================================

  private handleFullSchema(): Response {
    const nouns = this.loadNouns()
    const schema: Record<string, unknown> = {}

    for (const [name, nounSchema] of nouns) {
      schema[name] = {
        ...nounSchema,
        entityCount: this.getEntityCount(name),
      }
    }

    return this.json({ success: true, data: schema })
  }

  private handleSchemaGraph(): Response {
    const nouns = this.loadNouns()
    const nodes: { id: string; label: string; entityCount: number }[] = []
    const edges: { source: string; target: string; label: string; type: string }[] = []

    for (const [name, nounSchema] of nouns) {
      nodes.push({
        id: name,
        label: name,
        entityCount: this.getEntityCount(name),
      })

      for (const [relName, rel] of Object.entries(nounSchema.relationships)) {
        if (rel.targetType) {
          edges.push({
            source: name,
            target: rel.targetType,
            label: relName,
            type: rel.operator ?? '->',
          })
        }
      }
    }

    return this.json({ success: true, data: { nodes, edges } })
  }

  private getEntityCount(type: string): number {
    const row = this.sql.exec('SELECT COUNT(*) as cnt FROM entities WHERE type = ? AND deleted_at IS NULL', type).toArray()[0]
    return (row?.cnt as number) ?? 0
  }

  // =========================================================================
  // Tenant management
  // =========================================================================

  /**
   * Provision a new tenant. Stores metadata in the tenant_meta table.
   * The DO itself is lazily created — this just records that the tenant
   * was intentionally provisioned (vs. auto-created on first access).
   */
  private async handleProvisionTenant(request: Request): Promise<Response> {
    const body = (await request.json()) as { tenantId: string; name?: string; plan?: string }
    const tenantId = body.tenantId || request.headers.get('X-Tenant-ID')

    if (!tenantId) {
      return this.json({ success: false, error: 'Missing tenant ID' }, 400)
    }

    // Check if already provisioned
    const existing = this.sql.exec("SELECT value FROM tenant_meta WHERE key = 'status'").toArray()[0]
    if (existing && (existing.value as string) === 'active') {
      return this.json({ success: false, error: 'Tenant already provisioned' }, 409)
    }

    const now = new Date().toISOString()
    this.sql.exec("INSERT OR REPLACE INTO tenant_meta (key, value) VALUES ('tenantId', ?)", tenantId)
    this.sql.exec("INSERT OR REPLACE INTO tenant_meta (key, value) VALUES ('status', 'active')")
    this.sql.exec("INSERT OR REPLACE INTO tenant_meta (key, value) VALUES ('createdAt', ?)", now)
    if (body.name) {
      this.sql.exec("INSERT OR REPLACE INTO tenant_meta (key, value) VALUES ('name', ?)", body.name)
    }
    if (body.plan) {
      this.sql.exec("INSERT OR REPLACE INTO tenant_meta (key, value) VALUES ('plan', ?)", body.plan)
    }

    return this.json({
      success: true,
      data: {
        tenantId,
        name: body.name ?? tenantId,
        plan: body.plan ?? 'free',
        status: 'active',
        contextUrl: `https://headless.ly/~${tenantId}`,
        createdAt: now,
      },
    }, 201)
  }

  /**
   * Get tenant info from the metadata table.
   */
  private handleTenantInfo(request: Request): Response {
    const rows = this.sql.exec('SELECT key, value FROM tenant_meta').toArray()

    if (rows.length === 0) {
      return this.json({ success: false, error: 'Tenant not found' }, 404)
    }

    const meta: Record<string, string> = {}
    for (const row of rows) {
      meta[row.key as string] = row.value as string
    }

    return this.json({
      success: true,
      data: {
        tenantId: meta.tenantId ?? request.headers.get('X-Tenant-ID'),
        name: meta.name ?? meta.tenantId,
        plan: meta.plan ?? 'free',
        status: meta.status ?? 'active',
        contextUrl: `https://headless.ly/~${meta.tenantId ?? request.headers.get('X-Tenant-ID')}`,
        createdAt: meta.createdAt,
      },
    })
  }

  /**
   * Get tenant statistics: entity counts by type, total event count, noun count.
   */
  private handleTenantStats(request: Request): Response {
    const tenantId = request.headers.get('X-Tenant-ID')

    // Entity counts by type
    const entityRows = this.sql.exec('SELECT type, COUNT(*) as cnt FROM entities WHERE deleted_at IS NULL GROUP BY type').toArray()
    const entityCounts: Record<string, number> = {}
    let totalEntities = 0
    for (const row of entityRows) {
      const count = row.cnt as number
      entityCounts[row.type as string] = count
      totalEntities += count
    }

    // Total events
    const eventRow = this.sql.exec('SELECT COUNT(*) as cnt FROM events').toArray()[0]
    const totalEvents = (eventRow?.cnt as number) ?? 0

    // Total nouns
    const nounRow = this.sql.exec('SELECT COUNT(*) as cnt FROM nouns').toArray()[0]
    const totalNouns = (nounRow?.cnt as number) ?? 0

    // Total relationships
    const relRow = this.sql.exec('SELECT COUNT(*) as cnt FROM relationships').toArray()[0]
    const totalRelationships = (relRow?.cnt as number) ?? 0

    // Total hooks
    const hookRow = this.sql.exec('SELECT COUNT(*) as cnt FROM hooks').toArray()[0]
    const totalHooks = (hookRow?.cnt as number) ?? 0

    return this.json({
      success: true,
      data: {
        tenantId,
        nouns: totalNouns,
        entities: {
          total: totalEntities,
          byType: entityCounts,
        },
        events: totalEvents,
        relationships: totalRelationships,
        hooks: totalHooks,
      },
    })
  }

  /**
   * Deactivate a tenant (soft delete). Sets status to 'deactivated'.
   * Does NOT delete data — data can be reactivated later.
   */
  private handleDeactivateTenant(request: Request): Response {
    const rows = this.sql.exec("SELECT value FROM tenant_meta WHERE key = 'status'").toArray()

    if (rows.length === 0) {
      return this.json({ success: false, error: 'Tenant not found' }, 404)
    }

    const currentStatus = rows[0]!.value as string
    if (currentStatus === 'deactivated') {
      return this.json({ success: false, error: 'Tenant already deactivated' }, 400)
    }

    const now = new Date().toISOString()
    this.sql.exec("INSERT OR REPLACE INTO tenant_meta (key, value) VALUES ('status', 'deactivated')")
    this.sql.exec("INSERT OR REPLACE INTO tenant_meta (key, value) VALUES ('deactivatedAt', ?)", now)

    return this.json({
      success: true,
      data: {
        tenantId: request.headers.get('X-Tenant-ID'),
        status: 'deactivated',
        deactivatedAt: now,
      },
    })
  }

  // =========================================================================
  // Helpers
  // =========================================================================

  /**
   * Build the tenant context URL from request headers.
   * Falls back to a default URL if no tenant header is present.
   */
  private getTenantContextUrl(request: Request): string {
    const tenantId = request.headers.get('X-Tenant-ID') ?? request.headers.get('X-Tenant') ?? 'default'
    return `https://headless.ly/~${tenantId}`
  }

  private json(data: unknown, status = 200, extraHeaders?: Record<string, string>): Response {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      ...extraHeaders,
    }
    return new Response(JSON.stringify(data), { status, headers })
  }
}
