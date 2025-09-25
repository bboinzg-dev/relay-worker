'use strict';

const db = require('./db');

function normalizeIdentifier(name) {
  return String(name || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '');
}

function parseTableName(tableName) {
  const safe = String(tableName || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_.]/g, '');
  if (!safe) throw new Error('invalid table name');

  let schema = 'public';
  let table = safe;
  if (safe.includes('.')) {
    const parts = safe.split('.').filter(Boolean);
    if (!parts.length) throw new Error('invalid table name');
    if (parts.length === 1) {
      table = parts[0];
    } else {
      schema = parts[0];
      table = parts[parts.length - 1];
    }
  }

  schema = normalizeIdentifier(schema) || 'public';
  table = normalizeIdentifier(table);
  if (!table) throw new Error('invalid table name');

  return {
    schema,
    table,
    qualified: `${schema}.${table}`,
  };
}

function canonKeys(obj = {}) {
  const out = {};
  for (const [rawKey, value] of Object.entries(obj)) {
    const key = normalizeIdentifier(rawKey);
    if (!key) continue;
    if (!Object.prototype.hasOwnProperty.call(out, key)) {
      out[key] = value;
    }
  }
  return out;
}

const NO_UPDATE = new Set(['id', 'created_at', 'updated_at', 'brand_norm', 'code_norm']);

async function upsertByBrandCode(tableName, values = {}) {
  const { schema, table, qualified } = parseTableName(tableName);

  const brand = values?.brand ?? null;
  const code = values?.code ?? null;
  const rest = { ...values };
  const brandNormInput = rest.brand_norm;
  const codeNormInput = rest.code_norm;
  delete rest.brand;
  delete rest.code;
  delete rest.brand_norm;
  delete rest.code_norm;

  const payload = canonKeys({
    brand,
    code,
    brand_norm: brand ? String(brand).toLowerCase() : brandNormInput ?? null,
    code_norm: code ? String(code).toLowerCase() : codeNormInput ?? null,
    ...rest,
  });

  const cols = Object.keys(payload);
  if (!cols.length) return null;

  const meta = await db.query(
    `select column_name, is_generated
       from information_schema.columns
      where table_schema=$1 and table_name=$2`,
    [schema, table],
  );
  const allowed = new Set(meta.rows.map((r) => String(r.column_name).toLowerCase()));
  const generated = new Set(
    meta.rows
      .filter((r) => String(r.is_generated || '').toUpperCase() === 'ALWAYS')
      .map((r) => String(r.column_name).toLowerCase()),
  );

  const insertCols = [];
  const insertVals = [];
  for (const col of cols) {
    if (!allowed.has(col)) continue;
    if (generated.has(col)) continue;
    insertCols.push(col);
    insertVals.push(payload[col]);
  }
  if (!insertCols.length) return null;

  const params = insertCols.map((_, i) => `$${i + 1}`);
  const updates = insertCols
    .filter((col) => !NO_UPDATE.has(col))
    .map((col) => `${col}=EXCLUDED.${col}`);

  const sql = `
    insert into ${qualified} (${insertCols.join(',')})
    values (${params.join(',')})
    on conflict (brand_norm, code_norm)
    do update set ${updates.length ? `${updates.join(', ')}, ` : ''}updated_at=now()
    returning *`;

  const res = await db.query(sql, insertVals);
  return res.rows?.[0] || null;
}

module.exports = { upsertByBrandCode };
