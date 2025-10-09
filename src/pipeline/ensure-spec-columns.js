'use strict';

const path = require('node:path');

function tryRequire(paths) {
  const errors = [];
  for (const p of paths) {
    try {
      return require(p);
    } catch (err) {
      if (err?.code === 'MODULE_NOT_FOUND' && typeof err?.message === 'string' && err.message.includes(p)) {
        errors.push(err);
        continue;
      }
      throw err;
    }
  }
  const error = new Error(`MODULE_NOT_FOUND: ${paths.join(' | ')}`);
  error.code = 'MODULE_NOT_FOUND';
  error.attempts = errors.map((e) => e?.message || String(e));
  throw error;
}

const db = tryRequire([
  path.join(__dirname, '../../db'),
  path.join(__dirname, '../db'),
  path.join(__dirname, './db'),
  path.join(process.cwd(), 'db'),
]);

const TYPE_MAP = {
  int: 'integer',
  integer: 'integer',
  numeric: 'numeric',
  number: 'numeric',
    float: 'double precision',
    double: 'double precision',
    'double precision': 'double precision',
    decimal: 'numeric',
  bool: 'boolean',
  boolean: 'boolean',
  text: 'text',
  string: 'text',
};

function normKey(k) {
  return String(k || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '');
}

async function getColumnsOf(qualified) {
  const [schema, table] = qualified.includes('.')
    ? qualified.split('.', 2)
    : ['public', qualified];
  const { rows } = await db.query(
    `SELECT lower(column_name) AS col
       FROM information_schema.columns
      WHERE table_schema = $1
        AND table_name   = $2`,
    [schema, table]
  );
  return new Set(rows.map((r) => r.col));
}

async function ensureSpecColumnsForBlueprint(qualifiedTable, blueprint) {
  const have = await getColumnsOf(qualifiedTable);

  const allowed = Array.isArray(blueprint?.allowedKeys) ? blueprint.allowedKeys : [];
  const variants = Array.isArray(blueprint?.ingestOptions?.variant_keys)
    ? blueprint.ingestOptions.variant_keys
    : Array.isArray(blueprint?.variant_keys)
      ? blueprint.variant_keys
      : [];
      const fieldMeta = blueprint?.fields || blueprint?.fields_json || {};
      const fieldKeys = Object.keys(fieldMeta);

  const wantedList = [
    ...new Set(
      [...allowed, ...variants, ...fieldKeys]
        .map(normKey)
        .filter(Boolean)
    ),
  ];

  const toAdd = [];
  for (const key of wantedList) {
    if (have.has(key)) continue;
    const rawType = fieldMeta[key]?.type ?? fieldMeta[key] ?? 'text';
    const pgType = TYPE_MAP[String(rawType).toLowerCase()] || 'text';
    toAdd.push({ key, pgType });
  }

  if (!toAdd.length) return { added: 0 };

  for (const { key, pgType } of toAdd) {
    await db.query(
      `ALTER TABLE ${qualifiedTable} ADD COLUMN IF NOT EXISTS "${key}" ${pgType}`
    );
  }
  return { added: toAdd.length, columns: toAdd };
}

module.exports = { ensureSpecColumnsForBlueprint, getColumnsOf };