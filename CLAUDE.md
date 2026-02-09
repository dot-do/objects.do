# CLAUDE.md - objects.do

## What This Is

objects.do is the managed Digital Object service — the hosted runtime for Noun() entities with verb conjugation, event sourcing, time travel, and schema-driven APIs.

Part of the `.do` infrastructure ecosystem.

## Structure

```
src/
  index.ts              # Worker entry point (Hono)
  types.ts              # Type definitions
  do/objects-do.ts      # Main Durable Object (SQLite-backed)
  lib/
    parse.ts            # Noun definition parser
    linguistic.ts       # Verb/noun conjugation
    tenant.ts           # Tenant resolution middleware
    integration-dispatch.ts  # Integration hooks
    do-router.ts        # DO routing helper
  routes/
    nouns.ts            # Noun management
    verbs.ts            # Verb conjugation API
    entities.ts         # Entity CRUD
    schema.ts           # Schema discovery
    events.ts           # Event log
    subscriptions.ts    # Event subscriptions
    tenants.ts          # Tenant management
    integrations.ts     # Integration hooks
test/
  integration-dispatch.test.ts
  tenant.test.ts
```

## Commands

```bash
pnpm dev          # Local dev (wrangler dev)
pnpm deploy       # Deploy to Cloudflare
pnpm test         # Run tests (vitest)
pnpm typecheck    # TypeScript check
pnpm types        # Generate Cloudflare types
```

## Code Style

- No semicolons
- Single quotes
- 2-space indent
- Print width: 160

## Beads Issue Tracking

This project uses Beads (`.beads/`) for issue tracking.

```bash
bd ready                 # Show issues ready to work
bd list --status=open    # All open issues
bd show <id>             # Detailed issue view
bd create --title="..." --type=task --priority=2
bd update <id> --status=in_progress
bd close <id>            # Mark complete
bd sync                  # Sync with git
```

### Session Close Protocol

Before saying "done":
```bash
git status          # Check changes
git add <files>     # Stage code
bd sync             # Commit beads
git commit          # Commit code
git push            # Push to remote
```
