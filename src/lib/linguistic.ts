/**
 * Linguistic utilities for auto-deriving noun and verb forms
 *
 * Self-contained port of digital-objects/linguistic.ts so objects.do
 * has zero runtime dependencies beyond Hono.
 */

/**
 * Derive noun forms from a PascalCase name
 */
export function deriveNoun(name: string): { singular: string; plural: string; slug: string } {
  const words = name
    .replace(/([A-Z])/g, ' $1')
    .trim()
    .toLowerCase()
  const singular = words
  const slug = words.replace(/\s+/g, '-')
  const plural = pluralize(singular)

  return { singular, plural, slug }
}

/**
 * Pluralize a word
 */
export function pluralize(word: string): string {
  const parts = word.split(' ')
  if (parts.length > 1) {
    const lastIdx = parts.length - 1
    parts[lastIdx] = pluralize(parts[lastIdx]!)
    return parts.join(' ')
  }

  const w = word.toLowerCase()

  const irregulars: Record<string, string> = {
    person: 'people',
    child: 'children',
    man: 'men',
    woman: 'women',
    foot: 'feet',
    tooth: 'teeth',
    goose: 'geese',
    mouse: 'mice',
    ox: 'oxen',
    index: 'indices',
    vertex: 'vertices',
    matrix: 'matrices',
  }

  if (irregulars[w]) return irregulars[w]!

  if (/[sxz]$/.test(w) || /[sc]h$/.test(w)) return w + 'es'
  if (/[^aeiou]y$/.test(w)) return w.slice(0, -1) + 'ies'
  if (/f$/.test(w)) return w.slice(0, -1) + 'ves'
  if (/fe$/.test(w)) return w.slice(0, -2) + 'ves'

  return w + 's'
}

/**
 * Derive verb conjugations from base form
 */
export function deriveVerb(name: string): {
  action: string
  act: string
  activity: string
  event: string
  reverseBy: string
  reverseAt: string
} {
  const base = name.toLowerCase()

  const irregulars: Record<string, { act: string; activity: string; event: string }> = {
    write: { act: 'writes', activity: 'writing', event: 'written' },
    read: { act: 'reads', activity: 'reading', event: 'read' },
    run: { act: 'runs', activity: 'running', event: 'run' },
    begin: { act: 'begins', activity: 'beginning', event: 'begun' },
    do: { act: 'does', activity: 'doing', event: 'done' },
    go: { act: 'goes', activity: 'going', event: 'gone' },
    have: { act: 'has', activity: 'having', event: 'had' },
    be: { act: 'is', activity: 'being', event: 'been' },
    set: { act: 'sets', activity: 'setting', event: 'set' },
    get: { act: 'gets', activity: 'getting', event: 'got' },
    put: { act: 'puts', activity: 'putting', event: 'put' },
    cut: { act: 'cuts', activity: 'cutting', event: 'cut' },
    hit: { act: 'hits', activity: 'hitting', event: 'hit' },
  }

  if (irregulars[base]) {
    const irr = irregulars[base]!
    return {
      action: base,
      act: irr.act,
      activity: irr.activity,
      event: irr.event,
      reverseBy: `${irr.event}By`,
      reverseAt: `${irr.event}At`,
    }
  }

  let act: string
  let activity: string
  let event: string

  if (base.endsWith('s') || base.endsWith('x') || base.endsWith('z') || base.endsWith('ch') || base.endsWith('sh')) {
    act = base + 'es'
  } else if (base.endsWith('y') && !/[aeiou]y$/.test(base)) {
    act = base.slice(0, -1) + 'ies'
  } else {
    act = base + 's'
  }

  if (base.endsWith('e') && !base.endsWith('ee')) {
    activity = base.slice(0, -1) + 'ing'
  } else if (base.endsWith('ie')) {
    activity = base.slice(0, -2) + 'ying'
  } else if (/[^aeiou][aeiou][bcdfghlmnprstvwz]$/.test(base) && base.length <= 6) {
    activity = base + base[base.length - 1] + 'ing'
  } else {
    activity = base + 'ing'
  }

  if (base.endsWith('e')) {
    event = base + 'd'
  } else if (base.endsWith('y') && !/[aeiou]y$/.test(base)) {
    event = base.slice(0, -1) + 'ied'
  } else if (/[^aeiou][aeiou][bcdfghlmnprstvwz]$/.test(base) && base.length <= 6) {
    event = base + base[base.length - 1] + 'ed'
  } else {
    event = base + 'ed'
  }

  return {
    action: base,
    act,
    activity,
    event,
    reverseBy: `${event}By`,
    reverseAt: `${event}At`,
  }
}
