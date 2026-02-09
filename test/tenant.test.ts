/**
 * Tests for tenant resolution
 *
 * Verifies that resolveTenant correctly handles all URL patterns:
 * - X-Tenant header
 * - /~:tenant path segment
 * - Journey subdomains (build, launch, grow, scale, experiment, automate)
 * - System subdomains (crm, billing, projects, etc.)
 * - Industry subdomains (healthcare, construction, etc.)
 * - Service subdomains (db, code, objects — no journey/system/industry context)
 * - No tenant (returns null)
 */

import { describe, it, expect } from 'vitest'
import { resolveTenant, extractSubdomain, extractTenantFromPath, stripTenantPrefix } from '../src/lib/tenant'

// ---------------------------------------------------------------------------
// extractSubdomain
// ---------------------------------------------------------------------------

describe('extractSubdomain', () => {
  it('should extract subdomain from *.headless.ly', () => {
    expect(extractSubdomain('crm.headless.ly')).toBe('crm')
  })

  it('should extract subdomain from *.headless.ly with port', () => {
    expect(extractSubdomain('crm.headless.ly:8787')).toBe('crm')
  })

  it('should return null for bare headless.ly', () => {
    expect(extractSubdomain('headless.ly')).toBeNull()
  })

  it('should return null for objects.do', () => {
    expect(extractSubdomain('objects.do')).toBeNull()
  })

  it('should be case-insensitive', () => {
    expect(extractSubdomain('CRM.Headless.ly')).toBe('crm')
    expect(extractSubdomain('Build.Headless.ly')).toBe('build')
  })
})

// ---------------------------------------------------------------------------
// extractTenantFromPath
// ---------------------------------------------------------------------------

describe('extractTenantFromPath', () => {
  it('should extract tenant from /~tenant path', () => {
    expect(extractTenantFromPath('/~acme')).toBe('acme')
  })

  it('should extract tenant from /~tenant/... path', () => {
    expect(extractTenantFromPath('/~acme/entities/Contact')).toBe('acme')
  })

  it('should return null if no /~ prefix', () => {
    expect(extractTenantFromPath('/entities/Contact')).toBeNull()
  })

  it('should return null for empty path', () => {
    expect(extractTenantFromPath('/')).toBeNull()
  })
})

// ---------------------------------------------------------------------------
// stripTenantPrefix
// ---------------------------------------------------------------------------

describe('stripTenantPrefix', () => {
  it('should strip /~tenant prefix', () => {
    expect(stripTenantPrefix('/~acme/entities/Contact')).toBe('/entities/Contact')
  })

  it('should return / when only /~tenant', () => {
    expect(stripTenantPrefix('/~acme')).toBe('/')
  })

  it('should leave paths without /~ prefix unchanged', () => {
    expect(stripTenantPrefix('/entities/Contact')).toBe('/entities/Contact')
  })
})

// ---------------------------------------------------------------------------
// resolveTenant
// ---------------------------------------------------------------------------

describe('resolveTenant', () => {
  it('should resolve from X-Tenant header', () => {
    const req = new Request('https://objects.do/entities/Contact', {
      headers: { 'X-Tenant': 'acme' },
    })
    const ctx = resolveTenant(req)
    expect(ctx).not.toBeNull()
    expect(ctx?.tenantId).toBe('acme')
    expect(ctx?.contextUrl).toBe('https://headless.ly/~acme')
  })

  it('should resolve from /~tenant path', () => {
    const req = new Request('https://objects.do/~acme/entities/Contact')
    const ctx = resolveTenant(req)
    expect(ctx).not.toBeNull()
    expect(ctx?.tenantId).toBe('acme')
    expect(ctx?.contextUrl).toBe('https://headless.ly/~acme')
  })

  it('should prefer X-Tenant header over path', () => {
    const req = new Request('https://objects.do/~other/entities/Contact', {
      headers: { 'X-Tenant': 'acme' },
    })
    const ctx = resolveTenant(req)
    expect(ctx?.tenantId).toBe('acme')
  })

  it('should resolve journey from build subdomain', () => {
    const req = new Request('https://build.headless.ly/~acme/entities/Project')
    const ctx = resolveTenant(req)
    expect(ctx).not.toBeNull()
    expect(ctx?.tenantId).toBe('acme')
    expect(ctx?.journey).toBe('build')
    expect(ctx?.system).toBeUndefined()
    expect(ctx?.industry).toBeUndefined()
  })

  it('should resolve journey from launch subdomain', () => {
    const req = new Request('https://launch.headless.ly/~acme/entities/Campaign')
    const ctx = resolveTenant(req)
    expect(ctx?.tenantId).toBe('acme')
    expect(ctx?.journey).toBe('launch')
  })

  it('should resolve journey from grow subdomain', () => {
    const req = new Request('https://grow.headless.ly/~acme/entities/Contact')
    const ctx = resolveTenant(req)
    expect(ctx?.tenantId).toBe('acme')
    expect(ctx?.journey).toBe('grow')
  })

  it('should resolve journey from scale subdomain', () => {
    const req = new Request('https://scale.headless.ly/~acme/entities/Metric')
    const ctx = resolveTenant(req)
    expect(ctx?.tenantId).toBe('acme')
    expect(ctx?.journey).toBe('scale')
  })

  it('should resolve journey from experiment subdomain', () => {
    const req = new Request('https://experiment.headless.ly/~acme/entities/Experiment')
    const ctx = resolveTenant(req)
    expect(ctx?.tenantId).toBe('acme')
    expect(ctx?.journey).toBe('experiment')
  })

  it('should resolve journey from automate subdomain', () => {
    const req = new Request('https://automate.headless.ly/~acme/entities/Workflow')
    const ctx = resolveTenant(req)
    expect(ctx?.tenantId).toBe('acme')
    expect(ctx?.journey).toBe('automate')
  })

  it('should resolve system from crm subdomain', () => {
    const req = new Request('https://crm.headless.ly/~acme/entities/Contact')
    const ctx = resolveTenant(req)
    expect(ctx).not.toBeNull()
    expect(ctx?.tenantId).toBe('acme')
    expect(ctx?.system).toBe('crm')
    expect(ctx?.journey).toBeUndefined()
    expect(ctx?.industry).toBeUndefined()
  })

  it('should resolve system from billing subdomain', () => {
    const req = new Request('https://billing.headless.ly/~acme/entities/Invoice')
    const ctx = resolveTenant(req)
    expect(ctx?.tenantId).toBe('acme')
    expect(ctx?.system).toBe('billing')
  })

  it('should resolve system from analytics subdomain', () => {
    const req = new Request('https://analytics.headless.ly/~acme/entities/Event')
    const ctx = resolveTenant(req)
    expect(ctx?.tenantId).toBe('acme')
    expect(ctx?.system).toBe('analytics')
  })

  it('should resolve system from projects subdomain', () => {
    const req = new Request('https://projects.headless.ly/~acme/entities/Issue')
    const ctx = resolveTenant(req)
    expect(ctx?.tenantId).toBe('acme')
    expect(ctx?.system).toBe('projects')
  })

  it('should resolve industry from healthcare subdomain', () => {
    const req = new Request('https://healthcare.headless.ly/~acme/entities/Contact')
    const ctx = resolveTenant(req)
    expect(ctx).not.toBeNull()
    expect(ctx?.tenantId).toBe('acme')
    expect(ctx?.industry).toBe('healthcare')
    expect(ctx?.journey).toBeUndefined()
    expect(ctx?.system).toBeUndefined()
  })

  it('should resolve industry from construction subdomain', () => {
    const req = new Request('https://construction.headless.ly/~acme/entities/Contact')
    const ctx = resolveTenant(req)
    expect(ctx?.tenantId).toBe('acme')
    expect(ctx?.industry).toBe('construction')
  })

  it('should not set journey/system/industry for service subdomains', () => {
    const req = new Request('https://db.headless.ly/~acme/entities/Contact')
    const ctx = resolveTenant(req)
    expect(ctx).not.toBeNull()
    expect(ctx?.tenantId).toBe('acme')
    expect(ctx?.journey).toBeUndefined()
    expect(ctx?.system).toBeUndefined()
    expect(ctx?.industry).toBeUndefined()
  })

  it('should not set journey/system/industry for objects.do', () => {
    const req = new Request('https://objects.do/~acme/entities/Contact')
    const ctx = resolveTenant(req)
    expect(ctx).not.toBeNull()
    expect(ctx?.tenantId).toBe('acme')
    expect(ctx?.journey).toBeUndefined()
    expect(ctx?.system).toBeUndefined()
    expect(ctx?.industry).toBeUndefined()
  })

  it('should return null when no tenant provided', () => {
    const req = new Request('https://objects.do/health')
    const ctx = resolveTenant(req)
    expect(ctx).toBeNull()
  })

  it('should return null for bare headless.ly without tenant', () => {
    const req = new Request('https://headless.ly/entities/Contact')
    const ctx = resolveTenant(req)
    expect(ctx).toBeNull()
  })

  it('should resolve tenant from headless.ly with /~tenant path', () => {
    const req = new Request('https://headless.ly/~acme/entities/Contact')
    const ctx = resolveTenant(req)
    expect(ctx).not.toBeNull()
    expect(ctx?.tenantId).toBe('acme')
    // bare headless.ly has no subdomain context
    expect(ctx?.journey).toBeUndefined()
    expect(ctx?.system).toBeUndefined()
    expect(ctx?.industry).toBeUndefined()
  })

  it('should handle tenant with hyphens', () => {
    const req = new Request('https://objects.do/~my-startup/entities/Contact')
    const ctx = resolveTenant(req)
    expect(ctx?.tenantId).toBe('my-startup')
    expect(ctx?.contextUrl).toBe('https://headless.ly/~my-startup')
  })

  it('should handle all journey subdomains', () => {
    const journeys = ['build', 'launch', 'experiment', 'grow', 'automate', 'scale']
    for (const journey of journeys) {
      const req = new Request(`https://${journey}.headless.ly/~acme/test`)
      const ctx = resolveTenant(req)
      expect(ctx?.journey).toBe(journey)
      expect(ctx?.system).toBeUndefined()
      expect(ctx?.industry).toBeUndefined()
    }
  })

  it('should handle all system subdomains', () => {
    const systems = ['crm', 'billing', 'projects', 'content', 'support', 'analytics', 'marketing', 'experiments', 'platform']
    for (const system of systems) {
      const req = new Request(`https://${system}.headless.ly/~acme/test`)
      const ctx = resolveTenant(req)
      expect(ctx?.system).toBe(system)
      expect(ctx?.journey).toBeUndefined()
      expect(ctx?.industry).toBeUndefined()
    }
  })
})
