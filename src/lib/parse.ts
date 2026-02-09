/**
 * Noun definition parser — self-contained port of digital-objects/noun-parse.ts
 *
 * Parses property string patterns from Noun() definitions into
 * structured ParsedProperty objects.
 */

import type { ParsedProperty, FieldModifiers, VerbConjugation, StoredNounSchema } from '../types'
import { deriveNoun, deriveVerb } from './linguistic'

const RELATIONSHIP_REGEX = /^(.*?)\s*(<-|->|<~|~>)\s*(.+)$/
const ENUM_PIPE_REGEX = /\|/
const PASCAL_CASE_REGEX = /^[A-Z][a-zA-Z]+$/
const EXPLICIT_ENUM_REGEX = /^enum\(([^)]+)\)/
const DEFAULT_VALUE_REGEX = /=\s*"?([^"]*)"?\s*$/

const KNOWN_TYPES = new Set([
  'string',
  'number',
  'boolean',
  'date',
  'datetime',
  'json',
  'url',
  'email',
  'id',
  'text',
  'int',
  'decimal',
  'timestamp',
  'markdown',
  'float',
  'uuid',
  'ulid',
])

const CRUD_VERBS = new Set(['create', 'update', 'delete'])
const READ_VERBS = new Set(['get', 'find'])
const DEFAULT_CRUD_VERB_NAMES = ['create', 'update', 'delete'] as const

function isVerbDeclaration(key: string, value: string): boolean {
  return PASCAL_CASE_REGEX.test(value) && !KNOWN_TYPES.has(value.toLowerCase()) && !/^[A-Z]/.test(key) && !CRUD_VERBS.has(key) && !READ_VERBS.has(key)
}

function parseFieldModifiers(value: string): { type: string; modifiers: FieldModifiers; defaultValue?: string } {
  let remaining = value.trim()
  let defaultValue: string | undefined

  const defaultMatch = remaining.match(DEFAULT_VALUE_REGEX)
  if (defaultMatch?.[1] !== undefined) {
    defaultValue = defaultMatch[1].trim()
    remaining = remaining.replace(DEFAULT_VALUE_REGEX, '').trim()
  }

  const modifiers: FieldModifiers = {
    required: false,
    optional: false,
    indexed: false,
    unique: false,
    array: false,
  }

  if (remaining.endsWith('[]')) {
    modifiers.array = true
    remaining = remaining.slice(0, -2)
  }

  if (remaining.endsWith('##')) {
    modifiers.unique = true
    modifiers.indexed = true
    remaining = remaining.slice(0, -2)
  } else if (remaining.endsWith('#')) {
    modifiers.indexed = true
    remaining = remaining.slice(0, -1)
  }

  if (remaining.endsWith('!')) {
    modifiers.required = true
    remaining = remaining.slice(0, -1)
  } else if (remaining.endsWith('?')) {
    modifiers.optional = true
    remaining = remaining.slice(0, -1)
  }

  const enumMatch = remaining.match(EXPLICIT_ENUM_REGEX)
  if (enumMatch) remaining = 'enum'
  if (remaining.startsWith('decimal')) remaining = 'decimal'

  return { type: remaining, modifiers, defaultValue }
}

function parseRelationship(name: string, value: string): ParsedProperty {
  const match = value.match(RELATIONSHIP_REGEX)
  if (!match) return { name, kind: 'field', type: 'string' }

  const operator = match[2]!
  let targetPart = match[3]!.trim()
  const isArray = targetPart.endsWith('[]')
  if (isArray) targetPart = targetPart.slice(0, -2)

  let targetType: string
  let backref: string | undefined
  const dotIdx = targetPart.indexOf('.')
  if (dotIdx > 0) {
    targetType = targetPart.slice(0, dotIdx)
    backref = targetPart.slice(dotIdx + 1)
  } else {
    targetType = targetPart
  }

  return { name, kind: 'relationship', operator, targetType, backref, isArray }
}

function parseEnum(name: string, value: string): ParsedProperty {
  const values = value.split('|').map((v) => v.trim())
  return { name, kind: 'enum', enumValues: values }
}

function parseProperty(key: string, value: string | null): ParsedProperty {
  if (value === null) return { name: key, kind: 'disabled' }
  if (RELATIONSHIP_REGEX.test(value)) return parseRelationship(key, value)
  if (ENUM_PIPE_REGEX.test(value) && !value.startsWith('enum(')) return parseEnum(key, value)

  if (value.startsWith('enum(')) {
    const inner = value.match(EXPLICIT_ENUM_REGEX)
    if (inner?.[1]) {
      const values = inner[1].split(',').map((v) => v.trim().replace(/^["']|["']$/g, ''))
      const { defaultValue } = parseFieldModifiers(value)
      return { name: key, kind: 'enum', enumValues: values, defaultValue }
    }
  }

  if (isVerbDeclaration(key, value)) {
    return { name: key, kind: 'verb', verbAction: key }
  }

  const { type, modifiers, defaultValue } = parseFieldModifiers(value)
  return { name: key, kind: 'field', type, modifiers, defaultValue }
}

function conjugate(action: string): VerbConjugation {
  const derived = deriveVerb(action)
  return {
    action: derived.action,
    activity: derived.activity,
    event: derived.event,
    reverseBy: derived.reverseBy,
    reverseAt: derived.reverseAt,
  }
}

/**
 * Parse a raw Noun definition object into a StoredNounSchema
 */
export function parseNounDefinition(name: string, definition: Record<string, string | null>): StoredNounSchema {
  const derived = deriveNoun(name)
  const fields: Record<string, ParsedProperty> = {}
  const relationships: Record<string, ParsedProperty> = {}
  const verbs: Record<string, VerbConjugation> = {}
  const disabledVerbs: string[] = []

  // Parse properties
  for (const [key, value] of Object.entries(definition)) {
    const parsed = parseProperty(key, value)
    switch (parsed.kind) {
      case 'disabled':
        disabledVerbs.push(key)
        break
      case 'relationship':
        relationships[key] = parsed
        break
      case 'verb': {
        // Custom verb — conjugate it
        verbs[key] = conjugate(key)
        break
      }
      case 'field':
      case 'enum':
        fields[key] = parsed
        break
    }
  }

  // Add default CRUD verbs (unless disabled)
  const disabledSet = new Set(disabledVerbs)
  for (const verb of DEFAULT_CRUD_VERB_NAMES) {
    if (!disabledSet.has(verb)) {
      verbs[verb] = conjugate(verb)
    }
  }

  return {
    name,
    singular: derived.singular,
    plural: derived.plural,
    slug: derived.slug,
    fields,
    relationships,
    verbs,
    disabledVerbs,
    raw: definition,
  }
}
