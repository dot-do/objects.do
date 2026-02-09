/**
 * Type definitions for objects.do Worker
 */

/**
 * Standard API response envelope
 */
export type ApiResponse<T = unknown> = { success: true; data: T; meta?: { total?: number; limit?: number; offset?: number } } | { success: false; error: string }

/**
 * Service binding interface for Cloudflare Worker-to-Worker RPC.
 * Each binding supports fetch() for HTTP-based dispatch.
 */
export interface ServiceBinding {
  fetch(request: Request): Promise<Response>
}

/**
 * Environment bindings
 */
export interface Env {
  /** Durable Object for per-tenant digital object management */
  OBJECTS: DurableObjectNamespace
  /** R2 bucket for entity snapshots and backups */
  BUCKET: R2Bucket
  /** Stripe payment service binding (worker-stripe) */
  PAYMENTS: ServiceBinding
  /** GitHub repository service binding (worker-github) */
  REPO: ServiceBinding
  /** Integrations service binding */
  INTEGRATIONS: ServiceBinding
  /** OAuth service binding */
  OAUTH: ServiceBinding
  /** Events service binding */
  EVENTS: ServiceBinding
  /** Environment name */
  ENVIRONMENT: string
}

/**
 * Tenant context resolved from URL patterns, headers, or auth
 */
export interface TenantContext {
  /** The tenant identifier (e.g., 'acme') */
  tenantId: string
  /** Journey stage subdomain (build, launch, grow, scale, experiment, automate) */
  journey?: string
  /** System subdomain (crm, billing, projects, content, support, analytics, marketing, experiments, platform) */
  system?: string
  /** Industry subdomain (healthcare, construction, etc.) */
  industry?: string
  /** Canonical context URL for this tenant */
  contextUrl: string
}

/**
 * Hono context variables set by middleware
 */
export interface Variables {
  /** Tenant ID resolved from auth or X-Tenant header */
  tenant: string
  /** Full tenant context with journey/system/industry info */
  tenantContext: TenantContext
}

/**
 * Combined Hono env type
 */
export type AppEnv = { Bindings: Env; Variables: Variables }

// ---------------------------------------------------------------------------
// Noun / Verb / Entity types (mirrors digital-objects but JSON-serializable)
// ---------------------------------------------------------------------------

/**
 * Field modifier flags
 */
export interface FieldModifiers {
  required: boolean
  optional: boolean
  indexed: boolean
  unique: boolean
  array: boolean
}

/**
 * Property kind
 */
export type PropertyKind = 'field' | 'relationship' | 'enum' | 'verb' | 'disabled'

/**
 * Serializable parsed property
 */
export interface ParsedProperty {
  name: string
  kind: PropertyKind
  type?: string
  modifiers?: FieldModifiers
  defaultValue?: string
  enumValues?: string[]
  operator?: string
  targetType?: string
  backref?: string
  isArray?: boolean
  verbAction?: string
}

/**
 * Full verb conjugation (JSON-serializable)
 */
export interface VerbConjugation {
  action: string
  activity: string
  event: string
  reverseBy: string
  reverseAt: string
}

/**
 * Stored noun schema (JSON-serializable version of NounSchema)
 */
export interface StoredNounSchema {
  name: string
  singular: string
  plural: string
  slug: string
  fields: Record<string, ParsedProperty>
  relationships: Record<string, ParsedProperty>
  verbs: Record<string, VerbConjugation>
  disabledVerbs: string[]
  raw: Record<string, string | null>
}

/**
 * Entity instance with meta-fields
 */
export interface NounInstance {
  $id: string
  $type: string
  $context: string
  $version: number
  $createdAt: string
  $updatedAt: string
  [key: string]: unknown
}

/**
 * Event emitted by verb execution
 *
 * Uses $ prefix for meta-fields to be consistent with NounInstance and
 * the event types in @headlessly/events.
 */
export interface VerbEvent {
  $id: string
  $type: string
  entityType: string
  entityId: string
  verb: string
  data: Record<string, unknown> | null
  timestamp: string
}

/**
 * Hook registration
 */
export interface Hook {
  noun: string
  verb: string
  phase: 'before' | 'after'
  code: string
  createdAt: string
}

/**
 * Noun definition input (what the client sends)
 */
export interface NounDefinitionPayload {
  name: string
  definition: Record<string, string | null>
}

// ---------------------------------------------------------------------------
// RPC Stub interface — typed methods on ObjectsDO called via Workers RPC
// ---------------------------------------------------------------------------

import type { FullEvent } from './do/objects-do'

/**
 * Typed stub interface for ObjectsDO RPC calls.
 *
 * Route handlers obtain a stub via `getStub()` and call methods directly
 * instead of constructing HTTP requests.
 */
export interface ObjectsStub {
  // Nouns
  defineNoun(body: { name: string; definition: Record<string, string | null> }): Promise<{ success: boolean; data?: StoredNounSchema; error?: string; status: number }>
  listNouns(): Promise<{ success: boolean; data: StoredNounSchema[] }>
  getNounSchema(name: string): Promise<{ success: boolean; data?: StoredNounSchema; error?: string; status: number }>

  // Verbs
  listVerbs(): Promise<{ success: boolean; data: Record<string, { conjugation: VerbConjugation; nouns: string[] }> }>
  getVerb(verb: string): Promise<{ success: boolean; data?: { noun: string; conjugation: VerbConjugation }[]; error?: string; status: number }>
  conjugate(body: { verb: string }): Promise<{ success: boolean; data?: Record<string, string>; error?: string; status: number }>

  // Entities
  createEntity(type: string, data: Record<string, unknown>, opts?: { tenantId?: string; contextUrl?: string }): Promise<{ success: boolean; data?: NounInstance; error?: string; meta?: { eventId: string }; status: number }>
  getEntity(type: string, id: string): Promise<{ success: boolean; data?: NounInstance; error?: string; etag?: string; status: number }>
  listEntities(type: string, params: { limit?: number; offset?: number; filter?: string; sort?: string }): Promise<{ success: boolean; data?: NounInstance[]; error?: string; meta?: { total: number; limit: number; offset: number; hasMore: boolean }; status: number }>
  updateEntity(type: string, id: string, updates: Record<string, unknown>, opts?: { ifMatch?: string }): Promise<{ success: boolean; data?: NounInstance; error?: string; meta?: { eventId?: string; currentVersion?: number; expectedVersion?: number }; etag?: string; status: number }>
  deleteEntity(type: string, id: string): Promise<{ success: boolean; error?: string; meta?: { eventId: string }; status: number }>
  executeVerb(type: string, id: string, verb: string, verbData?: Record<string, unknown>): Promise<{ success: boolean; data?: NounInstance; error?: string; meta?: { event: FullEvent }; status: number }>
  registerHook(type: string, body: { verb: string; phase: 'before' | 'after'; code: string }): Promise<{ success: boolean; data?: { noun: string; verb: string; phase: string }; error?: string; status: number }>

  // Time Travel
  timeTravelGet(type: string, id: string, params: { asOf?: string; atVersion?: string }): Promise<{ success: boolean; data?: Record<string, unknown>; error?: string; status: number }>
  entityDiff(type: string, id: string, params: { from?: string; to?: string }): Promise<{ success: boolean; data?: Record<string, unknown>; error?: string; status: number }>
  entityHistory(entityType: string, entityId: string): Promise<{ success: boolean; data: FullEvent[] }>

  // Events
  queryEvents(params: { since?: string; type?: string; entityId?: string; verb?: string; limit?: number }): Promise<{ success: boolean; data: FullEvent[] }>
  getEvent(eventId: string): Promise<{ success: boolean; data?: FullEvent; error?: string; status: number }>
  getEventStream(params: { since?: string; types?: string; verbs?: string }): Promise<Response>

  // Subscriptions
  createSubscription(body: { pattern: string; mode: 'webhook' | 'websocket'; endpoint: string; secret?: string }): Promise<{ success: boolean; data?: Record<string, unknown>; error?: string; status: number }>
  listSubscriptions(): Promise<{ success: boolean; data: Record<string, unknown>[] }>
  deleteSubscription(subId: string): Promise<{ success: boolean; error?: string; status: number }>

  // Integration Hooks
  createIntegrationHook(body: { entityType: string; verb: string; service: string; method: string; config?: Record<string, unknown> }): Promise<{ success: boolean; data?: Record<string, unknown>; error?: string; status: number }>
  listIntegrationHooks(): Promise<{ success: boolean; data: { builtin: Record<string, unknown>[]; tenant: Record<string, unknown>[] } }>
  deleteIntegrationHook(hookId: string): Promise<{ success: boolean; error?: string; status: number }>
  queryDispatchLog(params: { eventId?: string; service?: string; status?: string; limit?: number }): Promise<{ success: boolean; data: Record<string, unknown>[] }>

  // Schema
  fullSchema(): Promise<{ success: boolean; data: Record<string, unknown> }>
  schemaGraph(): Promise<{ success: boolean; data: { nodes: Record<string, unknown>[]; edges: Record<string, unknown>[] } }>

  // Tenants
  provisionTenant(body: { tenantId: string; name?: string; plan?: string }): Promise<{ success: boolean; data?: Record<string, unknown>; error?: string; status: number }>
  tenantInfo(tenantId?: string): Promise<{ success: boolean; data?: Record<string, unknown>; error?: string; status: number }>
  tenantStats(tenantId?: string): Promise<{ success: boolean; data: Record<string, unknown> }>
  deactivateTenant(tenantId?: string): Promise<{ success: boolean; data?: Record<string, unknown>; error?: string; status: number }>
}
