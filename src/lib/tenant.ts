/**
 * Tenant resolution middleware
 *
 * Resolves tenant from multiple URL patterns and headers:
 *
 * 1. X-Tenant header
 * 2. /~:tenant path segment
 * 3. Subdomain context (journey, system, industry)
 * 4. Authorization: Bearer token (future — token claims)
 *
 * URL patterns → tenant ID:
 *   build.headless.ly/~acme      → tenant: 'acme', journey: 'build'
 *   crm.headless.ly/~acme        → tenant: 'acme', system: 'crm'
 *   headless.ly/~acme             → tenant: 'acme'
 *   objects.do/~acme              → tenant: 'acme'
 *   X-Tenant: acme header         → tenant: 'acme'
 */

import { createMiddleware } from 'hono/factory'
import type { AppEnv, TenantContext, ObjectsStub } from '../types'
import { getTenantStub } from './do-router'

// ---------------------------------------------------------------------------
// Known subdomain sets
// ---------------------------------------------------------------------------

/** Journey-phase subdomains — lifecycle stages for startups */
const JOURNEY_SUBDOMAINS = new Set(['build', 'launch', 'experiment', 'grow', 'automate', 'scale'])

/** System subdomains — product domain scopes */
const SYSTEM_SUBDOMAINS = new Set([
  'crm',
  'billing',
  'projects',
  'content',
  'support',
  'analytics',
  'marketing',
  'experiments',
  'platform',
])

/** Industry subdomains — NAICS-derived industry verticals */
const INDUSTRY_SUBDOMAINS = new Set([
  'healthcare',
  'health',
  'construction',
  'manufacturing',
  'agriculture',
  'mining',
  'utilities',
  'wholesale',
  'retail',
  'transportation',
  'information',
  'finance',
  'fintech',
  'insurance',
  'realestate',
  'professional',
  'management',
  'education',
  'arts',
  'entertainment',
  'hospitality',
  'accommodation',
  'government',
])

/** Service/utility subdomains that are NOT journey/system/industry */
const SERVICE_SUBDOMAINS = new Set(['db', 'code', 'objects', 'api', 'events', 'mcp', 'oauth', 'www'])

// ---------------------------------------------------------------------------
// Subdomain extraction
// ---------------------------------------------------------------------------

/**
 * Extract subdomain from the Host header.
 *
 * Handles:
 *   crm.headless.ly      → 'crm'
 *   crm.headless.ly:8787 → 'crm'
 *   headless.ly           → null
 *   objects.do             → null
 */
export function extractSubdomain(host: string): string | null {
  // Match *.headless.ly (with optional port)
  const headlesslyMatch = host.match(/^([^.]+)\.headless\.ly(?::\d+)?$/i)
  if (headlesslyMatch) return headlesslyMatch[1]!.toLowerCase()

  return null
}

/**
 * Classify a subdomain into journey, system, industry, or unknown.
 */
function classifySubdomain(sub: string): { journey?: string; system?: string; industry?: string } {
  const lower = sub.toLowerCase()

  if (JOURNEY_SUBDOMAINS.has(lower)) return { journey: lower }
  if (SYSTEM_SUBDOMAINS.has(lower)) return { system: lower }
  if (INDUSTRY_SUBDOMAINS.has(lower)) return { industry: lower }

  return {}
}

// ---------------------------------------------------------------------------
// Tenant resolution
// ---------------------------------------------------------------------------

/**
 * Extract tenant ID from /~:tenant path segment.
 *
 * @example
 *   /~acme/entities/Contact → 'acme'
 *   /entities/Contact       → null
 */
export function extractTenantFromPath(pathname: string): string | null {
  const match = pathname.match(/^\/~([^/]+)/)
  return match ? match[1]! : null
}

/**
 * Strip the /~:tenant prefix from a pathname so downstream routes
 * don't need to know about it.
 *
 * @example
 *   /~acme/entities/Contact → /entities/Contact
 *   /entities/Contact       → /entities/Contact
 */
export function stripTenantPrefix(pathname: string): string {
  return pathname.replace(/^\/~[^/]+/, '') || '/'
}

/**
 * Resolve tenant context from a Request.
 *
 * Resolution order:
 *   1. X-Tenant header
 *   2. /~:tenant path segment
 *   3. Return null if no tenant found
 *
 * Subdomain provides journey/system/industry context regardless
 * of how the tenant was resolved.
 */
export function resolveTenant(request: Request): TenantContext | null {
  const url = new URL(request.url)
  const host = url.host

  // 1. Try X-Tenant header
  let tenantId = request.headers.get('X-Tenant') ?? null

  // 2. Try /~:tenant path segment
  if (!tenantId) {
    tenantId = extractTenantFromPath(url.pathname)
  }

  // No tenant found
  if (!tenantId) return null

  // Resolve subdomain context
  const subdomain = extractSubdomain(host)
  let journey: string | undefined
  let system: string | undefined
  let industry: string | undefined

  if (subdomain && !SERVICE_SUBDOMAINS.has(subdomain)) {
    const classified = classifySubdomain(subdomain)
    journey = classified.journey
    system = classified.system
    industry = classified.industry
  }

  return {
    tenantId,
    journey,
    system,
    industry,
    contextUrl: `https://headless.ly/~${tenantId}`,
  }
}

// ---------------------------------------------------------------------------
// Hono middleware
// ---------------------------------------------------------------------------

/**
 * List of paths that do NOT require tenant resolution.
 * Health checks, root info, and tenant management endpoints.
 */
const TENANT_EXEMPT_PATHS = new Set(['/', '/health', '/tenants'])

/**
 * Check if a path is exempt from tenant resolution.
 */
function isTenantExempt(pathname: string): boolean {
  if (TENANT_EXEMPT_PATHS.has(pathname)) return true
  // POST /tenants is exempt (creating a tenant)
  if (pathname.startsWith('/tenants')) return true
  return false
}

/**
 * Tenant resolution middleware for Hono.
 *
 * Resolves tenant from request, sets context variables,
 * and returns 400 if no tenant is provided on routes that require it.
 */
export const tenantMiddleware = () =>
  createMiddleware<AppEnv>(async (c, next) => {
    const ctx = resolveTenant(c.req.raw)

    if (!ctx) {
      // Allow tenant-exempt paths through without tenant
      const url = new URL(c.req.url)
      if (isTenantExempt(url.pathname)) {
        return next()
      }

      return c.json(
        {
          success: false,
          error: 'Tenant required. Provide X-Tenant header or use /~{tenant}/ path prefix.',
        },
        400,
      )
    }

    c.set('tenant', ctx.tenantId)
    c.set('tenantContext', ctx)

    // Store the original path with /~:tenant prefix so route handlers
    // can access it if needed. The route handlers use the forward() helper
    // which constructs its own DO requests, so path rewriting is handled
    // at the DO forwarding layer rather than here.
    const url = new URL(c.req.url)
    const strippedPath = stripTenantPrefix(url.pathname)
    if (strippedPath !== url.pathname) {
      c.req.raw.headers.set('X-Original-Path', url.pathname)
    }

    return next()
  })

/**
 * Get the typed DO stub for the current tenant.
 * Convenience helper used by route handlers.
 */
export function getStub(c: { env: AppEnv['Bindings']; get: (key: 'tenant') => string }): ObjectsStub {
  const tenant = c.get('tenant')
  return getTenantStub(c.env, tenant)
}
