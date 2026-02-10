/**
 * ObjectsDO — Durable Object for managing Digital Objects per tenant
 *
 * Architecture:
 * - SQLite stores nouns (schemas), entities (data), events (audit log),
 *   relationships (graph edges via @dotdo/do rels.ts), hooks (code-as-data), and subscriptions
 * - Every verb execution emits a full NounEvent to the immutable event log
 *   with conjugation, before/after state, and monotonic sequence
 * - Events are dispatched to registered subscriptions (webhook/websocket/code)
 * - Entities use {type}_{sqid} IDs
 * - Soft-delete: entities are marked $deletedAt, never physically removed
 * - Hooks are stored as registrations but runtime code execution is disabled for security
 * - Time travel: entity state can be reconstructed at any version or timestamp
 * - CDC: Server-Sent Events stream for external consumers
 *
 * Public methods are the RPC interface — route handlers call them directly
 * via the DO stub, bypassing HTTP fetch.
 */

import { DurableObject } from 'cloudflare:workers'
import { parseNounDefinition } from '../lib/parse'
import { toPastParticiple, toGerund } from '../lib/linguistic'
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
import { createRels } from '../../../do/core/src/rels'
import { EventEmitter } from '../../../events/core/src/emitter'
import type { StoredNounSchema, NounInstance, VerbEvent, VerbConjugation, Hook } from '../types'
import type { Relationship } from '../../../do/core/src/rels'

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface FullEvent {
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
 * Delegates to lib/linguistic.ts (canonical copy from digital-objects) for
 * correct handling of consonant doubling, vowel-y, and irregular verbs.
 */
function conjugateVerb(verb: string): { action: string; activity: string; event: string } {
  return {
    action: verb,
    activity: toGerund(verb),
    event: toPastParticiple(verb),
  }
}

// ---------------------------------------------------------------------------
// Durable Object
// ---------------------------------------------------------------------------

export class ObjectsDO extends DurableObject<Cloudflare.Env> {
  private sql: SqlStorage

  /** Relationship graph edges — delegated to @dotdo/do rels.ts */
  private rels: ReturnType<typeof createRels>

  /** In-memory cache of noun schemas (hydrated from SQLite on first access) */
  private nounCache: Map<string, StoredNounSchema> | null = null

  /** Batched CDC event emitter — forwards events to events.do with retry and circuit breaker */
  private emitter: EventEmitter

  constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
    super(ctx, env)
    this.sql = ctx.storage.sql
    this.rels = createRels(this.sql)
    this.emitter = new EventEmitter(ctx, env as Record<string, unknown>, {
      endpoint: 'https://events.workers.do/ingest',
      batchSize: 100,
      flushIntervalMs: 1000,
      cdc: true,
      trackPrevious: true,
    })
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

    // Relationships table is managed by createRels() from @dotdo/do (this.rels)

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
  // Public RPC methods — called directly by route handlers via stub
  // =========================================================================

  // ---- Nouns ----

  defineNoun(body: { name: string; definition: Record<string, string | null> }): { success: boolean; data?: StoredNounSchema; error?: string; status: number } {
    if (!body.name || typeof body.name !== 'string') {
      return { success: false, error: 'Missing or invalid name', status: 400 }
    }
    if (!body.definition || typeof body.definition !== 'object' || Array.isArray(body.definition)) {
      return { success: false, error: 'Missing or invalid definition', status: 400 }
    }
    if (!/^[A-Z][a-zA-Z0-9]*$/.test(body.name)) {
      return { success: false, error: 'Noun name must be PascalCase (e.g., Contact, BlogPost)', status: 400 }
    }

    const schema = parseNounDefinition(body.name, body.definition)
    const schemaJson = JSON.stringify(schema)

    this.sql.exec("INSERT OR REPLACE INTO nouns (name, schema, created_at) VALUES (?, ?, datetime('now'))", schema.name, schemaJson)
    this.nounCache = null

    return { success: true, data: schema, status: 201 }
  }

  listNouns(): { success: boolean; data: StoredNounSchema[] } {
    const nouns = this.loadNouns()
    return { success: true, data: Array.from(nouns.values()) }
  }

  getNounSchema(name: string): { success: boolean; data?: StoredNounSchema; error?: string; status: number } {
    const noun = this.getNoun(name)
    if (!noun) {
      return { success: false, error: `Noun '${name}' not found`, status: 404 }
    }
    return { success: true, data: noun, status: 200 }
  }

  // ---- Verbs ----

  listVerbs(): { success: boolean; data: Record<string, { conjugation: VerbConjugation; nouns: string[] }> } {
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

    return { success: true, data: allVerbs }
  }

  getVerb(verb: string): { success: boolean; data?: { noun: string; conjugation: VerbConjugation }[]; error?: string; status: number } {
    const nouns = this.loadNouns()
    const matches: { noun: string; conjugation: VerbConjugation }[] = []

    for (const [nounName, schema] of nouns) {
      const conj = schema.verbs[verb]
      if (conj) {
        matches.push({ noun: nounName, conjugation: conj })
      }
      for (const c of Object.values(schema.verbs)) {
        if (c.activity === verb || c.event === verb) {
          matches.push({ noun: nounName, conjugation: c })
        }
      }
    }

    if (matches.length === 0) {
      return { success: false, error: `Verb '${verb}' not found on any noun`, status: 404 }
    }

    return { success: true, data: matches, status: 200 }
  }

  async conjugate(body: { verb: string }): Promise<{ success: boolean; data?: Record<string, string>; error?: string; status: number }> {
    const { deriveVerb } = await import('../lib/linguistic')

    if (!body.verb || typeof body.verb !== 'string') {
      return { success: false, error: 'Missing or invalid verb', status: 400 }
    }

    const derived = deriveVerb(body.verb)
    return {
      success: true,
      data: {
        action: derived.action,
        activity: derived.activity,
        event: derived.event,
        reverseBy: derived.reverseBy,
        reverseAt: derived.reverseAt,
      },
      status: 200,
    }
  }

  // ---- Entities ----

  createEntity(
    type: string,
    data: Record<string, unknown>,
    opts?: { tenantId?: string; contextUrl?: string },
  ): { success: boolean; data?: NounInstance; error?: string; meta?: { eventId: string }; status: number } {
    const noun = this.getNoun(type)
    if (!noun) {
      return { success: false, error: `Noun '${type}' is not defined. Define it first via POST /nouns`, status: 400 }
    }

    if (noun.disabledVerbs.includes('create')) {
      return { success: false, error: `Verb 'create' is disabled on ${type}`, status: 403 }
    }

    const id = (data.$id as string) || generateEntityId(type)
    const now = new Date().toISOString()
    const contextUrl = opts?.contextUrl ?? (opts?.tenantId ? `https://headless.ly/~${opts.tenantId}` : 'https://headless.ly/~default')

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

    const event = this.logEvent(type, id, 'create', entity, null, entity, contextUrl)

    return { success: true, data: entity, meta: { eventId: event.$id }, status: 201 }
  }

  getEntity(type: string, id: string): { success: boolean; data?: NounInstance; error?: string; etag?: string; status: number } {
    const row = this.sql.exec('SELECT data FROM entities WHERE id = ? AND type = ? AND deleted_at IS NULL', id, type).toArray()[0]

    if (!row) {
      return { success: false, error: 'Not found', status: 404 }
    }

    const entity = JSON.parse(row.data as string) as NounInstance
    return { success: true, data: entity, etag: `"${entity.$version}"`, status: 200 }
  }

  listEntities(
    type: string,
    params: { limit?: number; offset?: number; filter?: string; sort?: string },
  ): { success: boolean; data?: NounInstance[]; error?: string; meta?: { total: number; limit: number; offset: number; hasMore: boolean }; status: number } {
    const limit = Math.min(params.limit ?? 100, 1000)
    const offset = params.offset ?? 0

    // Build filter conditions to push into SQL WHERE clause
    const filterConditions: string[] = []
    const filterValues: (string | number | boolean | null)[] = []

    if (params.filter) {
      try {
        const filter = JSON.parse(params.filter) as Record<string, unknown>
        for (const [key, value] of Object.entries(filter)) {
          if (value === null) {
            filterConditions.push(`json_extract(data, '$.' || ?) IS NULL`)
            filterValues.push(key)
          } else if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
            filterConditions.push(`json_extract(data, '$.' || ?) = ?`)
            filterValues.push(key, value as string | number | boolean)
          }
        }
      } catch {
        return { success: false, error: 'Invalid filter JSON', status: 400 }
      }
    }

    const whereClause = 'WHERE type = ? AND deleted_at IS NULL' + (filterConditions.length > 0 ? ' AND ' + filterConditions.join(' AND ') : '')
    const whereValues: (string | number | boolean | null)[] = [type, ...filterValues]

    // Build ORDER BY — push sort into SQL via json_extract
    let orderBy = 'ORDER BY created_at DESC'
    const sortValues: (string | number | boolean | null)[] = []
    if (params.sort) {
      try {
        const sort = JSON.parse(params.sort) as Record<string, 1 | -1>
        const entry = Object.entries(sort)[0]
        if (entry) {
          const [field, dir] = entry
          const direction = dir === 1 ? 'ASC' : 'DESC'
          if (field === '$createdAt' || field === 'created_at') {
            orderBy = `ORDER BY created_at ${direction}`
          } else if (field === '$updatedAt' || field === 'updated_at') {
            orderBy = `ORDER BY updated_at ${direction}`
          } else {
            orderBy = `ORDER BY json_extract(data, '$.' || ?) ${direction}`
            sortValues.push(field)
          }
        }
      } catch {
        // Ignore invalid sort — fall back to default
      }
    }

    const query = `SELECT data FROM entities ${whereClause} ${orderBy} LIMIT ? OFFSET ?`
    const queryValues = [...whereValues, ...sortValues, limit, offset]

    const rows = this.sql.exec(query, ...queryValues).toArray()
    const entities = rows.map((r) => JSON.parse(r.data as string) as NounInstance)

    const countQuery = `SELECT COUNT(*) as cnt FROM entities ${whereClause}`
    const countRow = this.sql.exec(countQuery, ...whereValues).toArray()[0]
    const total = (countRow?.cnt as number) ?? 0

    return {
      success: true,
      data: entities,
      meta: { total, limit, offset, hasMore: offset + entities.length < total },
      status: 200,
    }
  }

  updateEntity(
    type: string,
    id: string,
    updates: Record<string, unknown>,
    opts?: { ifMatch?: string },
  ): { success: boolean; data?: NounInstance; error?: string; meta?: { eventId?: string; currentVersion?: number; expectedVersion?: number }; etag?: string; status: number } {
    const noun = this.getNoun(type)
    if (noun && noun.disabledVerbs.includes('update')) {
      return { success: false, error: `Verb 'update' is disabled on ${type}`, status: 403 }
    }

    const row = this.sql.exec('SELECT data, version FROM entities WHERE id = ? AND type = ? AND deleted_at IS NULL', id, type).toArray()[0]

    if (!row) {
      return { success: false, error: 'Not found', status: 404 }
    }

    const existing = JSON.parse(row.data as string) as NounInstance
    const currentVersion = row.version as number

    let expectedVersion: number | undefined
    if (updates.$version !== undefined) {
      expectedVersion = Number(updates.$version)
    } else if (opts?.ifMatch) {
      const parsed = parseInt(opts.ifMatch.replace(/"/g, ''), 10)
      if (!isNaN(parsed)) expectedVersion = parsed
    }

    if (expectedVersion !== undefined && expectedVersion !== currentVersion) {
      return {
        success: false,
        error: 'Version conflict',
        meta: { currentVersion, expectedVersion },
        etag: `"${currentVersion}"`,
        status: 409,
      }
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

    return { success: true, data: updated, meta: { eventId: event.$id }, etag: `"${nextVersion}"`, status: 200 }
  }

  deleteEntity(type: string, id: string): { success: boolean; error?: string; meta?: { eventId: string }; status: number } {
    const noun = this.getNoun(type)
    if (noun && noun.disabledVerbs.includes('delete')) {
      return { success: false, error: `Verb 'delete' is disabled on ${type}`, status: 403 }
    }

    const row = this.sql.exec('SELECT data FROM entities WHERE id = ? AND type = ? AND deleted_at IS NULL', id, type).toArray()[0]

    if (!row) {
      return { success: false, error: 'Not found', status: 404 }
    }

    const existing = JSON.parse(row.data as string) as NounInstance
    const now = new Date().toISOString()

    this.sql.exec('UPDATE entities SET deleted_at = ?, updated_at = ? WHERE id = ?', now, now, id)

    const event = this.logEvent(type, id, 'delete', null, existing, null, existing.$context)

    return { success: true, meta: { eventId: event.$id }, status: 200 }
  }

  executeVerb(
    type: string,
    id: string,
    verb: string,
    verbData?: Record<string, unknown>,
  ): { success: boolean; data?: NounInstance; error?: string; meta?: { event: FullEvent }; status: number } {
    const noun = this.getNoun(type)
    if (!noun) {
      return { success: false, error: `Noun '${type}' is not defined`, status: 400 }
    }

    const conj = noun.verbs[verb]
    if (!conj) {
      const verbEntry = Object.values(noun.verbs).find((v) => v.activity === verb || v.event === verb)
      if (!verbEntry) {
        return { success: false, error: `Verb '${verb}' is not defined on ${type}`, status: 400 }
      }
      return { success: false, error: `Use the action form '${verbEntry.action}' instead of '${verb}'`, status: 400 }
    }

    if (noun.disabledVerbs.includes(verb)) {
      return { success: false, error: `Verb '${verb}' is disabled on ${type}`, status: 403 }
    }

    const row = this.sql.exec('SELECT data, version FROM entities WHERE id = ? AND type = ? AND deleted_at IS NULL', id, type).toArray()[0]

    if (!row) {
      return { success: false, error: 'Not found', status: 404 }
    }

    const existing = JSON.parse(row.data as string) as NounInstance
    const currentVersion = row.version as number

    const beforeHooks = this.getHooks(type, verb, 'before')
    for (const hook of beforeHooks) {
      console.warn(`[ObjectsDO] Runtime hook execution disabled for security — hook: ${type}.${verb}:before`)
    }

    const now = new Date().toISOString()
    const nextVersion = currentVersion + 1

    const updated: NounInstance = {
      ...existing,
      ...(verbData ?? {}),
      $id: id,
      $type: type,
      $context: existing.$context,
      $version: nextVersion,
      $createdAt: existing.$createdAt,
      $updatedAt: now,
    }

    this.sql.exec('UPDATE entities SET data = ?, version = ?, updated_at = ? WHERE id = ?', JSON.stringify(updated), nextVersion, now, id)

    const event = this.logEvent(type, id, verb, updated, existing, updated, existing.$context)

    const afterHooks = this.getHooks(type, verb, 'after')
    for (const hook of afterHooks) {
      console.warn(`[ObjectsDO] Runtime hook execution disabled for security — hook: ${type}.${verb}:after`)
    }

    return { success: true, data: updated, meta: { event }, status: 200 }
  }

  registerHook(
    type: string,
    body: { verb: string; phase: 'before' | 'after'; code: string },
  ): { success: boolean; data?: { noun: string; verb: string; phase: string }; error?: string; status: number } {
    const noun = this.getNoun(type)
    if (!noun) {
      return { success: false, error: `Noun '${type}' is not defined`, status: 400 }
    }

    if (!body.verb || typeof body.verb !== 'string') {
      return { success: false, error: 'Missing or invalid verb', status: 400 }
    }
    if (!body.phase || (body.phase !== 'before' && body.phase !== 'after')) {
      return { success: false, error: "phase must be 'before' or 'after'", status: 400 }
    }
    if (!body.code || typeof body.code !== 'string') {
      return { success: false, error: 'Missing or invalid code', status: 400 }
    }

    if (!noun.verbs[body.verb]) {
      return { success: false, error: `Verb '${body.verb}' is not defined on ${type}`, status: 400 }
    }

    this.sql.exec('INSERT INTO hooks (noun, verb, phase, code) VALUES (?, ?, ?, ?)', type, body.verb, body.phase, body.code)

    return { success: true, data: { noun: type, verb: body.verb, phase: body.phase }, status: 201 }
  }

  // ---- Time Travel ----

  timeTravelGet(
    type: string,
    id: string,
    params: { asOf?: string; atVersion?: string },
  ): { success: boolean; data?: Record<string, unknown>; error?: string; status: number } {
    const asOf = params.asOf
    const atVersionStr = params.atVersion

    let eventsQuery = 'SELECT * FROM events WHERE entity_type = ? AND entity_id = ?'
    const values: (string | number)[] = [type, id]

    if (atVersionStr) {
      const atVersion = parseInt(atVersionStr, 10)
      if (isNaN(atVersion)) {
        return { success: false, error: 'Invalid atVersion parameter', status: 400 }
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
      return { success: false, error: 'No events found for this entity at the specified point in time', status: 404 }
    }

    const state = this.replayEvents(rows.map((r) => this.rowToFullEvent(r)))

    if (!state) {
      return { success: false, error: 'Could not reconstruct state', status: 404 }
    }

    return { success: true, data: state, status: 200 }
  }

  entityDiff(
    type: string,
    id: string,
    params: { from?: string; to?: string },
  ): {
    success: boolean
    data?: { before: Record<string, unknown> | null; after: Record<string, unknown> | null; events: FullEvent[]; changes: Array<{ field: string; from: unknown; to: unknown }> }
    error?: string
    status: number
  } {
    if (!params.from || !params.to) {
      return { success: false, error: 'Both from and to parameters are required', status: 400 }
    }

    const fromVersion = parseInt(params.from, 10)
    const toVersion = parseInt(params.to, 10)

    if (isNaN(fromVersion) || isNaN(toVersion)) {
      return { success: false, error: 'from and to must be valid integers', status: 400 }
    }

    const allRows = this.sql
      .exec('SELECT * FROM events WHERE entity_type = ? AND entity_id = ? ORDER BY sequence ASC', type, id)
      .toArray()

    if (allRows.length === 0) {
      return { success: false, error: 'No events found for this entity', status: 404 }
    }

    const allEvents = allRows.map((r) => this.rowToFullEvent(r))

    const fromEvents = allEvents.filter((e) => e.sequence <= fromVersion)
    const toEvents = allEvents.filter((e) => e.sequence <= toVersion)
    const betweenEvents = allEvents.filter((e) => e.sequence > fromVersion && e.sequence <= toVersion)

    const beforeState = this.replayEvents(fromEvents)
    const afterState = this.replayEvents(toEvents)

    const changes = this.computeChanges(beforeState, afterState)

    return {
      success: true,
      data: { before: beforeState, after: afterState, events: betweenEvents, changes },
      status: 200,
    }
  }

  entityHistory(entityType: string, entityId: string): { success: boolean; data: FullEvent[] } {
    const rows = this.sql
      .exec('SELECT * FROM events WHERE entity_type = ? AND entity_id = ? ORDER BY sequence ASC', entityType, entityId)
      .toArray()

    return { success: true, data: rows.map((r) => this.rowToFullEvent(r)) }
  }

  // ---- Events ----

  queryEvents(params: {
    since?: string
    type?: string
    entityId?: string
    verb?: string
    limit?: number
  }): { success: boolean; data: FullEvent[] } {
    const limit = Math.min(params.limit ?? 100, 1000)

    let query = 'SELECT * FROM events'
    const conditions: string[] = []
    const values: (string | number)[] = []

    if (params.since) {
      conditions.push('timestamp > ?')
      values.push(params.since)
    }
    if (params.type) {
      conditions.push('entity_type = ?')
      values.push(params.type)
    }
    if (params.entityId) {
      conditions.push('entity_id = ?')
      values.push(params.entityId)
    }
    if (params.verb) {
      conditions.push('verb = ?')
      values.push(params.verb)
    }

    if (conditions.length > 0) {
      query += ' WHERE ' + conditions.join(' AND ')
    }

    query += ' ORDER BY timestamp DESC LIMIT ?'
    values.push(limit)

    const rows = this.sql.exec(query, ...values).toArray()

    return { success: true, data: rows.map((r) => this.rowToFullEvent(r)) }
  }

  getEvent(eventId: string): { success: boolean; data?: FullEvent; error?: string; status: number } {
    const rows = this.sql.exec('SELECT * FROM events WHERE id = ?', eventId).toArray()

    if (rows.length === 0) {
      return { success: false, error: 'Event not found', status: 404 }
    }

    return { success: true, data: this.rowToFullEvent(rows[0]), status: 200 }
  }

  /**
   * Returns SSE stream response for CDC. This is the one method that still
   * returns a Response because SSE streams require raw Response construction.
   */
  getEventStream(params: { since?: string; types?: string; verbs?: string }): Response {
    const sinceId = params.since
    const types = params.types ? params.types.split(',') : null
    const verbs = params.verbs ? params.verbs.split(',') : null

    const sql = this.sql
    const encoder = new TextEncoder()

    let bufferedQuery = 'SELECT * FROM events'
    const conditions: string[] = []
    const values: (string | number)[] = []

    if (sinceId) {
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
        const rows = sql.exec(bufferedQuery, ...values).toArray()
        for (const row of rows) {
          const event = rowToEvent(row)
          const sseMsg = `id: ${event.$id}\nevent: ${event.$type}\ndata: ${JSON.stringify(event)}\n\n`
          controller.enqueue(encoder.encode(sseMsg))
        }

        controller.enqueue(encoder.encode(': heartbeat\n\n'))
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

  // ---- Subscriptions ----

  createSubscription(body: {
    pattern: string
    mode: 'webhook' | 'websocket'
    endpoint: string
    secret?: string
  }): { success: boolean; data?: Record<string, unknown>; error?: string; status: number } {
    if (!body.pattern || typeof body.pattern !== 'string') {
      return { success: false, error: 'Missing or invalid pattern', status: 400 }
    }
    if (!body.mode || (body.mode !== 'webhook' && body.mode !== 'websocket')) {
      return { success: false, error: "mode must be 'webhook' or 'websocket'", status: 400 }
    }
    if (!body.endpoint || typeof body.endpoint !== 'string') {
      return { success: false, error: 'Missing or invalid endpoint', status: 400 }
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

    return {
      success: true,
      data: {
        id,
        pattern: body.pattern,
        mode: body.mode,
        endpoint: body.endpoint,
        active: true,
        createdAt: now,
      },
      status: 201,
    }
  }

  listSubscriptions(): { success: boolean; data: Record<string, unknown>[] } {
    const rows = this.sql.exec('SELECT * FROM subscriptions ORDER BY created_at DESC').toArray()

    const subs = rows.map((r) => ({
      id: r.id as string,
      pattern: r.pattern as string,
      mode: r.mode as string,
      endpoint: r.endpoint as string,
      active: (r.active as number) === 1,
      createdAt: r.created_at as string,
    }))

    return { success: true, data: subs }
  }

  deleteSubscription(subId: string): { success: boolean; error?: string; status: number } {
    const rows = this.sql.exec('SELECT id FROM subscriptions WHERE id = ?', subId).toArray()

    if (rows.length === 0) {
      return { success: false, error: 'Subscription not found', status: 404 }
    }

    this.sql.exec('DELETE FROM subscriptions WHERE id = ?', subId)

    return { success: true, status: 200 }
  }

  // ---- Integration Hooks ----

  createIntegrationHook(body: {
    entityType: string
    verb: string
    service: string
    method: string
    config?: Record<string, unknown>
  }): { success: boolean; data?: Record<string, unknown>; error?: string; status: number } {
    if (!body.entityType || typeof body.entityType !== 'string') {
      return { success: false, error: 'Missing or invalid entityType', status: 400 }
    }
    if (!body.verb || typeof body.verb !== 'string') {
      return { success: false, error: 'Missing or invalid verb', status: 400 }
    }
    if (!body.service || typeof body.service !== 'string') {
      return { success: false, error: 'Missing or invalid service', status: 400 }
    }

    const validServices: ServiceName[] = ['PAYMENTS', 'REPO', 'INTEGRATIONS', 'OAUTH', 'EVENTS']
    if (!validServices.includes(body.service as ServiceName)) {
      return { success: false, error: `Invalid service. Must be one of: ${validServices.join(', ')}`, status: 400 }
    }

    if (!body.method || typeof body.method !== 'string') {
      return { success: false, error: 'Missing or invalid method (e.g., "POST /customers/sync")', status: 400 }
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

    return {
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
      status: 201,
    }
  }

  listIntegrationHooks(): { success: boolean; data: { builtin: Record<string, unknown>[]; tenant: Record<string, unknown>[] } } {
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

    return { success: true, data: { builtin: builtinList, tenant: tenantHooks } }
  }

  deleteIntegrationHook(hookId: string): { success: boolean; error?: string; status: number } {
    if (hookId.startsWith('builtin:')) {
      return { success: false, error: 'Cannot delete built-in hooks', status: 403 }
    }

    const rows = this.sql.exec('SELECT id FROM integration_hooks WHERE id = ?', hookId).toArray()

    if (rows.length === 0) {
      return { success: false, error: 'Integration hook not found', status: 404 }
    }

    this.sql.exec('DELETE FROM integration_hooks WHERE id = ?', hookId)

    return { success: true, status: 200 }
  }

  queryDispatchLog(params: {
    eventId?: string
    service?: string
    status?: string
    limit?: number
  }): { success: boolean; data: Record<string, unknown>[] } {
    const limit = Math.min(params.limit ?? 100, 1000)

    let query = 'SELECT * FROM dispatch_log'
    const conditions: string[] = []
    const values: (string | number)[] = []

    if (params.eventId) {
      conditions.push('event_id = ?')
      values.push(params.eventId)
    }
    if (params.service) {
      conditions.push('service = ?')
      values.push(params.service)
    }
    if (params.status) {
      conditions.push('status = ?')
      values.push(params.status)
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

    return { success: true, data: results }
  }

  // ---- Schema ----

  fullSchema(): { success: boolean; data: Record<string, unknown> } {
    const nouns = this.loadNouns()
    const schema: Record<string, unknown> = {}

    for (const [name, nounSchema] of nouns) {
      schema[name] = {
        ...nounSchema,
        entityCount: this.getEntityCount(name),
      }
    }

    return { success: true, data: schema }
  }

  schemaGraph(): { success: boolean; data: { nodes: { id: string; label: string; entityCount: number }[]; edges: { source: string; target: string; label: string; type: string }[] } } {
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

    return { success: true, data: { nodes, edges } }
  }

  // ---- Relationships ----

  createRelationship(
    sourceType: string,
    sourceId: string,
    relationship: { type: string; targetType: string; targetId: string; data?: Record<string, unknown> },
  ): { success: true; data: { id: string; from: string; predicate: string; to: string; createdAt: number } } {
    const from = `${sourceType}_${sourceId}`
    const to = `${relationship.targetType}_${relationship.targetId}`
    const rel = this.rels.add(from, relationship.type, to)
    return { success: true, data: { id: rel.id, from: rel.from, predicate: rel.predicate, to: rel.to, createdAt: rel.createdAt } }
  }

  getRelationships(
    type: string,
    id: string,
    options?: { relType?: string; targetType?: string; direction?: 'outgoing' | 'incoming' | 'both' },
  ): { success: true; data: Relationship[] } {
    const entityKey = `${type}_${id}`
    const direction = options?.direction ?? 'both'

    let outgoing: Relationship[] = []
    let incoming: Relationship[] = []

    if (direction === 'outgoing' || direction === 'both') {
      outgoing = this.rels.relationships(entityKey, options?.relType)
      if (options?.targetType) {
        outgoing = outgoing.filter((r) => r.to.startsWith(`${options.targetType}_`))
      }
    }

    if (direction === 'incoming' || direction === 'both') {
      incoming = this.rels.references(entityKey, options?.relType)
      if (options?.targetType) {
        incoming = incoming.filter((r) => r.from.startsWith(`${options.targetType}_`))
      }
    }

    const data = direction === 'outgoing' ? outgoing : direction === 'incoming' ? incoming : [...outgoing, ...incoming]
    return { success: true, data }
  }

  deleteRelationship(relationshipId: string): { success: true } {
    this.rels.delete(relationshipId)
    return { success: true }
  }

  // ---- OpenAPI ----

  openAPISpec(): Record<string, unknown> {
    const result = this.listNouns()
    const nouns = result.data

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

  // ---- Tenant Management ----

  provisionTenant(body: {
    tenantId: string
    name?: string
    plan?: string
  }): { success: boolean; data?: Record<string, unknown>; error?: string; status: number } {
    const tenantId = body.tenantId
    if (!tenantId) {
      return { success: false, error: 'Missing tenant ID', status: 400 }
    }

    const existing = this.sql.exec("SELECT value FROM tenant_meta WHERE key = 'status'").toArray()[0]
    if (existing && (existing.value as string) === 'active') {
      return { success: false, error: 'Tenant already provisioned', status: 409 }
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

    return {
      success: true,
      data: {
        tenantId,
        name: body.name ?? tenantId,
        plan: body.plan ?? 'free',
        status: 'active',
        contextUrl: `https://headless.ly/~${tenantId}`,
        createdAt: now,
      },
      status: 201,
    }
  }

  tenantInfo(tenantId?: string): { success: boolean; data?: Record<string, unknown>; error?: string; status: number } {
    const rows = this.sql.exec('SELECT key, value FROM tenant_meta').toArray()

    if (rows.length === 0) {
      return { success: false, error: 'Tenant not found', status: 404 }
    }

    const meta: Record<string, string> = {}
    for (const row of rows) {
      meta[row.key as string] = row.value as string
    }

    return {
      success: true,
      data: {
        tenantId: meta.tenantId ?? tenantId,
        name: meta.name ?? meta.tenantId,
        plan: meta.plan ?? 'free',
        status: meta.status ?? 'active',
        contextUrl: `https://headless.ly/~${meta.tenantId ?? tenantId}`,
        createdAt: meta.createdAt,
      },
      status: 200,
    }
  }

  tenantStats(tenantId?: string): { success: boolean; data: Record<string, unknown> } {
    const entityRows = this.sql.exec('SELECT type, COUNT(*) as cnt FROM entities WHERE deleted_at IS NULL GROUP BY type').toArray()
    const entityCounts: Record<string, number> = {}
    let totalEntities = 0
    for (const row of entityRows) {
      const count = row.cnt as number
      entityCounts[row.type as string] = count
      totalEntities += count
    }

    const eventRow = this.sql.exec('SELECT COUNT(*) as cnt FROM events').toArray()[0]
    const totalEvents = (eventRow?.cnt as number) ?? 0

    const nounRow = this.sql.exec('SELECT COUNT(*) as cnt FROM nouns').toArray()[0]
    const totalNouns = (nounRow?.cnt as number) ?? 0

    const relRow = this.sql.exec('SELECT COUNT(*) as cnt FROM _rels').toArray()[0]
    const totalRelationships = (relRow?.cnt as number) ?? 0

    const hookRow = this.sql.exec('SELECT COUNT(*) as cnt FROM hooks').toArray()[0]
    const totalHooks = (hookRow?.cnt as number) ?? 0

    return {
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
    }
  }

  deactivateTenant(tenantId?: string): { success: boolean; data?: Record<string, unknown>; error?: string; status: number } {
    const rows = this.sql.exec("SELECT value FROM tenant_meta WHERE key = 'status'").toArray()

    if (rows.length === 0) {
      return { success: false, error: 'Tenant not found', status: 404 }
    }

    const currentStatus = rows[0]!.value as string
    if (currentStatus === 'deactivated') {
      return { success: false, error: 'Tenant already deactivated', status: 400 }
    }

    const now = new Date().toISOString()
    this.sql.exec("INSERT OR REPLACE INTO tenant_meta (key, value) VALUES ('status', 'deactivated')")
    this.sql.exec("INSERT OR REPLACE INTO tenant_meta (key, value) VALUES ('deactivatedAt', ?)", now)

    return {
      success: true,
      data: {
        tenantId,
        status: 'deactivated',
        deactivatedAt: now,
      },
      status: 200,
    }
  }

  // =========================================================================
  // DO lifecycle — alarm handler for EventEmitter retries
  // =========================================================================

  async alarm(): Promise<void> {
    await this.emitter.handleAlarm()
  }

  // =========================================================================
  // Private helpers
  // =========================================================================

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

    // Emit CDC change to events.do via batched EventEmitter (replaces the built-in EVENTS hook)
    this.emitter.emitChange(
      verb === 'create' ? 'insert' : verb === 'delete' ? 'delete' : 'update',
      entityType,
      entityId,
      afterState ?? undefined,
      beforeState ?? undefined,
    )

    this.dispatchToSubscriptions(event)
    this.dispatchIntegrations(event, contextUrl ?? `https://headless.ly/~default`)

    return event
  }

  // =========================================================================
  // Integration hook dispatch
  // =========================================================================

  private getServiceBindings(): ServiceBindings {
    const bindings: ServiceBindings = {}
    const env = this.env as Record<string, unknown>
    const serviceNames: ServiceName[] = ['PAYMENTS', 'REPO', 'INTEGRATIONS', 'OAUTH']
    for (const name of serviceNames) {
      if (env[name] && typeof (env[name] as { fetch?: unknown }).fetch === 'function') {
        bindings[name] = env[name] as { fetch(request: Request): Promise<Response> }
      }
    }
    return bindings
  }

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

  private dispatchIntegrations(event: FullEvent, contextUrl: string): void {
    const services = this.getServiceBindings()

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

    dispatchIntegrationHooks(services, payload, tenantHooks)
      .then((results) => {
        this.logDispatchResults(event.$id, results)
      })
      .catch((err) => {
        console.error('[ObjectsDO] Integration dispatch failed:', err instanceof Error ? err.message : err)
      })
  }

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
        this.dispatchWebhook(sub, event).catch(() => {
          // Swallow errors — don't break the main flow
        })
      }
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
        action: (row.conjugation_action as string) || (row.verb as string),
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

  private getEntityCount(type: string): number {
    const row = this.sql.exec('SELECT COUNT(*) as cnt FROM entities WHERE type = ? AND deleted_at IS NULL', type).toArray()[0]
    return (row?.cnt as number) ?? 0
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
}
