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
