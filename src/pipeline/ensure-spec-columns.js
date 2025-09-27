'use strict';

const db = require('../utils/db');

const TYPE_MAP = {
  int: 'integer',
  integer: 'integer',
  numeric: 'numeric',
  number: 'numeric',
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

  const wantedList = [
    ...new Set(
      [...allowed, ...variants]
        .map(normKey)
        .filter(Boolean)
    ),
  ];

  const fieldMeta = blueprint?.fields || blueprint?.fields_json || {};
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