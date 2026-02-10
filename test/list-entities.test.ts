/**
 * Integration tests for ObjectsDO.listEntities filter + pagination behavior
 *
 * Validates that filters are pushed into SQL via json_extract() instead of
 * being applied in JS after LIMIT. This prevents the old bug where entities
 * matching a filter but beyond the LIMIT boundary were silently dropped.
 *
 * Uses better-sqlite3 as a stand-in for Cloudflare's SqlStorage so that the
 * real ObjectsDO SQL queries execute against a genuine SQLite engine.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import Database from 'better-sqlite3'

// ---------------------------------------------------------------------------
// Mock cloudflare:workers — ObjectsDO extends DurableObject which is only
// available in the Workers runtime. We provide a minimal stub.
// ---------------------------------------------------------------------------

vi.mock('cloudflare:workers', () => ({
  DurableObject: class DurableObject {
    ctx: unknown
    env: unknown
    constructor(ctx: unknown, env: unknown) {
      this.ctx = ctx
      this.env = env
    }
  },
}))

// ---------------------------------------------------------------------------
// Mock EventEmitter — the emitter reaches out to events.do which is
// irrelevant for these tests.
// ---------------------------------------------------------------------------

vi.mock('../../../events/core/src/emitter', () => ({
  EventEmitter: class EventEmitter {
    constructor() {}
    emitChange() {}
    handleAlarm() {}
  },
}))

// ---------------------------------------------------------------------------
// SqlStorage mock backed by better-sqlite3
// ---------------------------------------------------------------------------

function createMockSqlStorage(db: InstanceType<typeof Database>) {
  // better-sqlite3 does not accept booleans — coerce to 0/1 like real SQLite
  const coerceBindings = (bindings: unknown[]) =>
    bindings.map((b) => (typeof b === 'boolean' ? (b ? 1 : 0) : b))

  return {
    exec(query: string, ...bindings: unknown[]) {
      // Cloudflare's SqlStorage.exec handles both read and write queries.
      // For writes (INSERT, UPDATE, DELETE, CREATE, ALTER, DROP), we use run().
      // For reads (SELECT), we use all().
      const trimmed = query.trim()

      // Handle multi-statement SQL (CREATE TABLE + CREATE INDEX, etc.)
      // by splitting on semicolons and executing each statement.
      const statements = trimmed
        .split(/;\s*/)
        .map((s) => s.trim())
        .filter((s) => s.length > 0)

      if (statements.length > 1 && bindings.length === 0) {
        for (const stmt of statements) {
          db.exec(stmt)
        }
        return {
          toArray() {
            return []
          },
          one() {
            return undefined
          },
          columnNames: [],
          rowsRead: 0,
          rowsWritten: 0,
        }
      }

      const isRead = /^\s*(SELECT|PRAGMA)/i.test(trimmed)

      if (isRead) {
        const stmt = db.prepare(trimmed)
        const rows = stmt.all(...coerceBindings(bindings))
        return {
          toArray() {
            return rows
          },
          one() {
            return rows[0]
          },
          columnNames: rows.length > 0 ? Object.keys(rows[0] as object) : [],
          rowsRead: rows.length,
          rowsWritten: 0,
        }
      } else {
        const stmt = db.prepare(trimmed)
        const info = stmt.run(...coerceBindings(bindings))
        return {
          toArray() {
            return []
          },
          one() {
            return undefined
          },
          columnNames: [],
          rowsRead: 0,
          rowsWritten: info.changes,
        }
      }
    },
  }
}

// ---------------------------------------------------------------------------
// Helper: create a fresh ObjectsDO instance with an in-memory SQLite backend
// ---------------------------------------------------------------------------

async function createTestDO() {
  const db = new Database(':memory:')

  const sqlStorage = createMockSqlStorage(db)

  const mockCtx = {
    storage: {
      sql: sqlStorage,
      getAlarm: () => null,
      setAlarm: () => {},
      get: () => undefined,
      put: () => {},
      delete: () => {},
      deleteAll: () => {},
      list: () => new Map(),
    },
    id: { toString: () => 'test-do-id' },
    waitUntil: () => {},
  }

  const mockEnv = {}

  // Dynamic import so vi.mock directives are applied before the module loads
  const { ObjectsDO } = await import('../src/do/objects-do')

  // Construct the DO — the constructor calls initSchema() which creates all tables
  const doInstance = new ObjectsDO(mockCtx as any, mockEnv as any)

  return doInstance
}

// ---------------------------------------------------------------------------
// Helper: define a Contact noun on the DO
// ---------------------------------------------------------------------------

function defineContactNoun(doInstance: any) {
  const result = doInstance.defineNoun({
    name: 'Contact',
    definition: {
      name: 'string!',
      email: 'string?#',
      stage: 'Lead | Qualified | Customer | Churned | Partner',
    },
  })
  expect(result.success).toBe(true)
  return result
}

// ---------------------------------------------------------------------------
// Helper: create N entities with a given stage
// ---------------------------------------------------------------------------

function createContacts(doInstance: any, count: number, stage: string, namePrefix?: string) {
  const entities = []
  for (let i = 0; i < count; i++) {
    const result = doInstance.createEntity('Contact', {
      name: `${namePrefix ?? stage} ${i + 1}`,
      email: `${stage.toLowerCase()}${i + 1}@test.com`,
      stage,
    })
    expect(result.success).toBe(true)
    entities.push(result.data)
  }
  return entities
}

// ===========================================================================
// Tests
// ===========================================================================

describe('ObjectsDO.listEntities — filter + pagination', () => {
  let doInstance: any

  beforeEach(async () => {
    doInstance = await createTestDO()
    defineContactNoun(doInstance)
  })

  // -------------------------------------------------------------------------
  // 1. Basic filter returns only matching entities
  // -------------------------------------------------------------------------

  it('should return only matching entities when filter is provided', () => {
    createContacts(doInstance, 5, 'Lead')
    createContacts(doInstance, 5, 'Customer')

    const result = doInstance.listEntities('Contact', {
      filter: '{"stage":"Lead"}',
      limit: 100,
    })

    expect(result.success).toBe(true)
    expect(result.data).toHaveLength(5)
    for (const entity of result.data!) {
      expect(entity.stage).toBe('Lead')
    }
    expect(result.meta!.total).toBe(5)
  })

  // -------------------------------------------------------------------------
  // 2. Filter + pagination returns correct page with correct total
  // -------------------------------------------------------------------------

  it('should paginate correctly within filtered results', () => {
    createContacts(doInstance, 20, 'Lead')
    createContacts(doInstance, 10, 'Customer')

    // First page
    const page1 = doInstance.listEntities('Contact', {
      filter: '{"stage":"Lead"}',
      limit: 5,
      offset: 0,
    })

    expect(page1.success).toBe(true)
    expect(page1.data).toHaveLength(5)
    for (const entity of page1.data!) {
      expect(entity.stage).toBe('Lead')
    }
    expect(page1.meta!.total).toBe(20)
    expect(page1.meta!.hasMore).toBe(true)

    // Second page
    const page2 = doInstance.listEntities('Contact', {
      filter: '{"stage":"Lead"}',
      limit: 5,
      offset: 5,
    })

    expect(page2.success).toBe(true)
    expect(page2.data).toHaveLength(5)
    for (const entity of page2.data!) {
      expect(entity.stage).toBe('Lead')
    }
    expect(page2.meta!.total).toBe(20)
    expect(page2.meta!.hasMore).toBe(true)

    // Verify page1 and page2 have different entities
    const page1Ids = new Set(page1.data!.map((e: any) => e.$id))
    const page2Ids = new Set(page2.data!.map((e: any) => e.$id))
    for (const id of page2Ids) {
      expect(page1Ids.has(id)).toBe(false)
    }
  })

  // -------------------------------------------------------------------------
  // 3. THE CRITICAL TEST — filter does not drop entities beyond LIMIT
  //
  // The old buggy code would:
  //   SELECT ... LIMIT 10   (gets 10 rows, most are Customer)
  //   then JS filter         (drops non-Lead rows → returns maybe 0-1 Lead)
  //
  // The fixed code pushes filter into WHERE:
  //   SELECT ... WHERE json_extract(data,'$.stage')='Lead' LIMIT 10
  //   (returns up to 10 Leads regardless of where they sit in the table)
  // -------------------------------------------------------------------------

  it('should find all matching entities even when buried after non-matching rows', () => {
    // Create 95 Customers first — they'll have earlier rowids
    createContacts(doInstance, 95, 'Customer')
    // Create 5 Leads — they're "buried" at the end of the table
    createContacts(doInstance, 5, 'Lead')

    const result = doInstance.listEntities('Contact', {
      filter: '{"stage":"Lead"}',
      limit: 10,
    })

    expect(result.success).toBe(true)
    // The old code would return 0 Leads here because the first 10 rows
    // fetched by LIMIT would all be Customers. The fix pushes the filter
    // into SQL so all 5 Leads are found.
    expect(result.data).toHaveLength(5)
    for (const entity of result.data!) {
      expect(entity.stage).toBe('Lead')
    }
    expect(result.meta!.total).toBe(5)
  })

  // -------------------------------------------------------------------------
  // 4. Sort pushed to SQL via json_extract
  // -------------------------------------------------------------------------

  it('should sort by a data field using json_extract', () => {
    // Create contacts with specific names to verify sort order
    const names = ['Charlie', 'Alice', 'Eve', 'Bob', 'Diana']
    for (const name of names) {
      doInstance.createEntity('Contact', { name, stage: 'Lead' })
    }

    const result = doInstance.listEntities('Contact', {
      sort: '{"name":1}',
      limit: 100,
    })

    expect(result.success).toBe(true)
    expect(result.data).toHaveLength(5)

    const returnedNames = result.data!.map((e: any) => e.name)
    expect(returnedNames).toEqual(['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'])
  })

  it('should sort descending when direction is -1', () => {
    const names = ['Charlie', 'Alice', 'Eve', 'Bob', 'Diana']
    for (const name of names) {
      doInstance.createEntity('Contact', { name, stage: 'Lead' })
    }

    const result = doInstance.listEntities('Contact', {
      sort: '{"name":-1}',
      limit: 100,
    })

    expect(result.success).toBe(true)
    const returnedNames = result.data!.map((e: any) => e.name)
    expect(returnedNames).toEqual(['Eve', 'Diana', 'Charlie', 'Bob', 'Alice'])
  })

  // -------------------------------------------------------------------------
  // 5. No filter returns all entities (regression)
  // -------------------------------------------------------------------------

  it('should return all entities when no filter is provided', () => {
    createContacts(doInstance, 5, 'Lead')
    createContacts(doInstance, 5, 'Customer')

    const result = doInstance.listEntities('Contact', { limit: 100 })

    expect(result.success).toBe(true)
    expect(result.data).toHaveLength(10)
    expect(result.meta!.total).toBe(10)
  })

  // -------------------------------------------------------------------------
  // 6. Empty filter object returns all entities
  // -------------------------------------------------------------------------

  it('should return all entities when filter is an empty object', () => {
    createContacts(doInstance, 3, 'Lead')
    createContacts(doInstance, 3, 'Customer')

    const result = doInstance.listEntities('Contact', {
      filter: '{}',
      limit: 100,
    })

    expect(result.success).toBe(true)
    expect(result.data).toHaveLength(6)
    expect(result.meta!.total).toBe(6)
  })

  // -------------------------------------------------------------------------
  // 7. Invalid filter JSON returns error
  // -------------------------------------------------------------------------

  it('should return error for invalid filter JSON', () => {
    createContacts(doInstance, 3, 'Lead')

    const result = doInstance.listEntities('Contact', {
      filter: 'not-valid-json',
      limit: 100,
    })

    expect(result.success).toBe(false)
    expect(result.error).toContain('Invalid filter JSON')
    expect(result.status).toBe(400)
  })

  // -------------------------------------------------------------------------
  // 8. Filter with multiple fields (AND logic)
  // -------------------------------------------------------------------------

  it('should apply multiple filter fields with AND logic', () => {
    doInstance.createEntity('Contact', { name: 'Alice', stage: 'Lead' })
    doInstance.createEntity('Contact', { name: 'Bob', stage: 'Lead' })
    doInstance.createEntity('Contact', { name: 'Alice', stage: 'Customer' })
    doInstance.createEntity('Contact', { name: 'Bob', stage: 'Customer' })

    const result = doInstance.listEntities('Contact', {
      filter: '{"name":"Alice","stage":"Lead"}',
      limit: 100,
    })

    expect(result.success).toBe(true)
    expect(result.data).toHaveLength(1)
    expect(result.data![0].name).toBe('Alice')
    expect(result.data![0].stage).toBe('Lead')
    expect(result.meta!.total).toBe(1)
  })

  // -------------------------------------------------------------------------
  // 9. Filter + sort together
  // -------------------------------------------------------------------------

  it('should filter and sort simultaneously', () => {
    doInstance.createEntity('Contact', { name: 'Charlie', stage: 'Lead' })
    doInstance.createEntity('Contact', { name: 'Alice', stage: 'Lead' })
    doInstance.createEntity('Contact', { name: 'Eve', stage: 'Customer' })
    doInstance.createEntity('Contact', { name: 'Bob', stage: 'Lead' })

    const result = doInstance.listEntities('Contact', {
      filter: '{"stage":"Lead"}',
      sort: '{"name":1}',
      limit: 100,
    })

    expect(result.success).toBe(true)
    expect(result.data).toHaveLength(3)
    const returnedNames = result.data!.map((e: any) => e.name)
    expect(returnedNames).toEqual(['Alice', 'Bob', 'Charlie'])
  })

  // -------------------------------------------------------------------------
  // 10. hasMore is false on the last page
  // -------------------------------------------------------------------------

  it('should set hasMore to false when all results have been returned', () => {
    createContacts(doInstance, 3, 'Lead')
    createContacts(doInstance, 10, 'Customer')

    const result = doInstance.listEntities('Contact', {
      filter: '{"stage":"Lead"}',
      limit: 10,
      offset: 0,
    })

    expect(result.success).toBe(true)
    expect(result.data).toHaveLength(3)
    expect(result.meta!.total).toBe(3)
    expect(result.meta!.hasMore).toBe(false)
  })

  // -------------------------------------------------------------------------
  // 11. Deleted entities are excluded from filtered results
  // -------------------------------------------------------------------------

  it('should exclude soft-deleted entities from filtered results', () => {
    const leads = createContacts(doInstance, 5, 'Lead')

    // Soft-delete two of the five Leads
    doInstance.deleteEntity('Contact', leads[0].$id)
    doInstance.deleteEntity('Contact', leads[1].$id)

    const result = doInstance.listEntities('Contact', {
      filter: '{"stage":"Lead"}',
      limit: 100,
    })

    expect(result.success).toBe(true)
    expect(result.data).toHaveLength(3)
    expect(result.meta!.total).toBe(3)
  })

  // -------------------------------------------------------------------------
  // 12. Numeric filter value
  // -------------------------------------------------------------------------

  it('should filter by numeric values', () => {
    // Define a Deal noun with a numeric field
    doInstance.defineNoun({
      name: 'Deal',
      definition: {
        title: 'string!',
        value: 'number!',
        stage: 'Open | Won | Lost',
      },
    })

    doInstance.createEntity('Deal', { title: 'Small Deal', value: 1000, stage: 'Open' })
    doInstance.createEntity('Deal', { title: 'Big Deal', value: 50000, stage: 'Open' })
    doInstance.createEntity('Deal', { title: 'Another Small', value: 1000, stage: 'Won' })

    const result = doInstance.listEntities('Deal', {
      filter: '{"value":1000}',
      limit: 100,
    })

    expect(result.success).toBe(true)
    expect(result.data).toHaveLength(2)
    for (const entity of result.data!) {
      expect(entity.value).toBe(1000)
    }
    expect(result.meta!.total).toBe(2)
  })

  // -------------------------------------------------------------------------
  // 13. Boolean filter value
  // -------------------------------------------------------------------------

  it('should filter by boolean values', () => {
    doInstance.defineNoun({
      name: 'FeatureFlag',
      definition: {
        key: 'string!',
        enabled: 'boolean!',
      },
    })

    doInstance.createEntity('FeatureFlag', { key: 'dark-mode', enabled: true })
    doInstance.createEntity('FeatureFlag', { key: 'new-ui', enabled: false })
    doInstance.createEntity('FeatureFlag', { key: 'beta', enabled: true })

    const result = doInstance.listEntities('FeatureFlag', {
      filter: '{"enabled":true}',
      limit: 100,
    })

    expect(result.success).toBe(true)
    expect(result.data).toHaveLength(2)
    for (const entity of result.data!) {
      expect(entity.enabled).toBe(true)
    }
  })

  // -------------------------------------------------------------------------
  // 14. Stress test — filter with large dataset
  // -------------------------------------------------------------------------

  it('should handle filter + pagination across a large dataset', () => {
    // Create 200 Customers and 50 Leads interleaved in batches
    for (let batch = 0; batch < 10; batch++) {
      createContacts(doInstance, 20, 'Customer', `Batch${batch}Customer`)
      createContacts(doInstance, 5, 'Lead', `Batch${batch}Lead`)
    }

    // Fetch page 1 of Leads (limit 10)
    const page1 = doInstance.listEntities('Contact', {
      filter: '{"stage":"Lead"}',
      limit: 10,
      offset: 0,
    })

    expect(page1.success).toBe(true)
    expect(page1.data).toHaveLength(10)
    expect(page1.meta!.total).toBe(50)
    expect(page1.meta!.hasMore).toBe(true)

    // Fetch remaining Leads
    const page2 = doInstance.listEntities('Contact', {
      filter: '{"stage":"Lead"}',
      limit: 100,
      offset: 10,
    })

    expect(page2.success).toBe(true)
    expect(page2.data).toHaveLength(40)
    expect(page2.meta!.total).toBe(50)
    expect(page2.meta!.hasMore).toBe(false)
  })

  // -------------------------------------------------------------------------
  // 15. Sort by $createdAt (built-in column)
  // -------------------------------------------------------------------------

  it('should sort by $createdAt ascending', () => {
    createContacts(doInstance, 5, 'Lead')

    const result = doInstance.listEntities('Contact', {
      sort: '{"$createdAt":1}',
      limit: 100,
    })

    expect(result.success).toBe(true)
    expect(result.data).toHaveLength(5)

    // Verify ascending order
    for (let i = 1; i < result.data!.length; i++) {
      expect(result.data![i].$createdAt >= result.data![i - 1].$createdAt).toBe(true)
    }
  })
})
