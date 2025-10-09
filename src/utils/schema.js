'use strict';

const crypto = require('node:crypto');
const db = require('../../db');

function isMinimalInsertEnabled() {
  return /^(1|true|on)$/i.test(String(process.env.ALLOW_MINIMAL_INSERT || '').trim());
}

function buildMinimalPnFallback(values = {}) {
  const uri = values?.datasheet_uri || values?.datasheet_url || values?.gcs_uri || values?.gcsUri || '';
  const brand = values?.brand || '';
  const code = values?.code || '';
  const series = values?.series || values?.series_code || '';
  const seed = String(uri || `${brand}:${code}:${series}` || '').trim();
  if (!seed) return null;
  const hash = crypto.createHash('sha1').update(seed).digest('hex');
  return `pdf:${hash.slice(0, 12)}`;
}

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

const FIELD_TYPE_MAP = {
  int: 'integer',
  integer: 'integer',
  number: 'numeric',
  numeric: 'numeric',
  float: 'double precision',
  double: 'double precision',
  'double precision': 'double precision',
  decimal: 'numeric',
  bool: 'boolean',
  boolean: 'boolean',
  json: 'jsonb',
  jsonb: 'jsonb',
  text: 'text',
  string: 'text',
  date: 'date',
  timestamp: 'timestamptz',
  timestamptz: 'timestamptz',
};

function normalizeFieldEntries(fields = {}) {
  const entries = [];
  if (Array.isArray(fields)) {
    for (const item of fields) {
      if (!item || typeof item !== 'object') continue;
      const key = normalizeIdentifier(item.name || item.key || item.field);
      if (!key) continue;
      const type = item.type ?? item.pgType ?? item.pg_type ?? item.data_type ?? 'text';
      entries.push([key, type]);
    }
    return entries;
  }

  if (fields && typeof fields === 'object') {
    for (const [rawKey, meta] of Object.entries(fields)) {
      const key = normalizeIdentifier(rawKey);
      if (!key) continue;
      if (meta && typeof meta === 'object' && !Array.isArray(meta)) {
        const type = meta.type ?? meta.pgType ?? meta.pg_type ?? meta.data_type ?? 'text';
        entries.push([key, type]);
      } else {
        entries.push([key, meta ?? 'text']);
      }
    }
  }
  return entries;
}

async function ensureSpecsTable(tableName, fields = {}) {
  const { schema, table, qualified } = parseTableName(tableName);

  await db.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`);
  await db.query(`
    CREATE TABLE IF NOT EXISTS ${qualified} (
      id           BIGSERIAL PRIMARY KEY,
      family_slug  text,
      brand        text,
      code         text,
      pn           text,
      series       text,
      series_code  text,
      display_name text,
      displayname  text,
      datasheet_uri text,
      datasheet_url text,
      image_uri    text,
      cover        text,
      source_gcs_uri text,
      verified_in_doc boolean,
      raw_json     jsonb,
      last_error   text,
      created_at   timestamptz DEFAULT now(),
      updated_at   timestamptz DEFAULT now()
    )
  `);

  const baseColumns = [
    { name: 'family_slug', type: 'text' },
    { name: 'brand', type: 'text' },
    { name: 'code', type: 'text' },
    { name: 'pn', type: 'text' },
    { name: 'series', type: 'text' },
    { name: 'series_code', type: 'text' },
    { name: 'datasheet_uri', type: 'text' },
    { name: 'datasheet_url', type: 'text' },
    { name: 'image_uri', type: 'text' },
    { name: 'cover', type: 'text' },
    { name: 'display_name', type: 'text' },
    { name: 'displayname', type: 'text' },
    { name: 'source_gcs_uri', type: 'text' },
    { name: 'verified_in_doc', type: 'boolean' },
    { name: 'raw_json', type: 'jsonb' },
    { name: 'last_error', type: 'text' },
    { name: 'created_at', type: 'timestamptz', defaultSql: 'now()' },
    { name: 'updated_at', type: 'timestamptz', defaultSql: 'now()' },
  ];

  for (const col of baseColumns) {
    const defaultClause = col.defaultSql ? ` DEFAULT ${col.defaultSql}` : '';
    await db.query(`ALTER TABLE ${qualified} ADD COLUMN IF NOT EXISTS "${col.name}" ${col.type}${defaultClause}`);
    if (col.defaultSql) {
      await db.query(`ALTER TABLE ${qualified} ALTER COLUMN "${col.name}" SET DEFAULT ${col.defaultSql}`);
    }
  }

  const generatedColumns = [
    { name: 'brand_norm', expression: 'lower(brand)' },
    { name: 'code_norm', expression: 'lower(code)' },
    { name: 'pn_norm', expression: 'lower(pn)' },
  ];

  for (const col of generatedColumns) {
    await db.query(
      `ALTER TABLE ${qualified} ADD COLUMN IF NOT EXISTS "${col.name}" text GENERATED ALWAYS AS (${col.expression}) STORED`
    );
  }

  const fieldEntries = normalizeFieldEntries(fields);
  const reserved = new Set([
    'id',
    ...baseColumns.map((c) => c.name),
    ...generatedColumns.map((c) => c.name),
  ]);

  for (const [key, typeInput] of fieldEntries) {
    if (!key || reserved.has(key)) continue;
    const mapped = FIELD_TYPE_MAP[String(typeInput).toLowerCase()] || 'text';
    await db.query(`ALTER TABLE ${qualified} ADD COLUMN IF NOT EXISTS "${key}" ${mapped}`);
  }

  await db.query(
    `CREATE UNIQUE INDEX IF NOT EXISTS ux_${table}_brandpn_expr ON ${qualified} (lower(brand), lower(pn))`
  );
}

const NO_UPDATE = new Set(['id', 'created_at', 'updated_at', 'brand_norm', 'code_norm', 'pn', 'pn_norm']);

async function upsertByBrandCode(tableName, values = {}) {
  const { schema, table, qualified } = parseTableName(tableName);

  const brand = values?.brand ?? null;
  let code = values?.code ?? null;
  let pn = values?.pn ?? values?.code ?? null;
  const rest = { ...values };
  const brandNormInput = rest.brand_norm;
  const codeNormInput = rest.code_norm;
  const pnNormInput = rest.pn_norm;
  delete rest.brand;
  delete rest.code;
  delete rest.pn;
  delete rest.brand_norm;
  delete rest.code_norm;
  delete rest.pn_norm;

  const allowMinimal = isMinimalInsertEnabled();
  if (typeof pn === 'string' && !pn.trim()) pn = null;
  if (!pn && allowMinimal) {
    const candidate = values?.series ?? values?.series_code ?? null;
    if (candidate && String(candidate).trim()) {
      pn = candidate;
    }
  }
  if (!pn && allowMinimal) {
    const fallbackPn = buildMinimalPnFallback(values);
    if (fallbackPn) pn = fallbackPn;
  }
  if (!pn) {
    throw new Error('pn required');
  }
  if (!code && pn) code = pn;

  const payload = canonKeys({
    brand,
    code,
    pn,
    brand_norm: brand ? String(brand).toLowerCase() : brandNormInput ?? null,
    code_norm: code ? String(code).toLowerCase() : codeNormInput ?? null,
    pn_norm: pn ? String(pn).toLowerCase() : pnNormInput ?? null,
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

  // Spec tables enforce uniqueness via the expression index (lower(brand), lower(pn)).
  // Prefer targeting the named index when available, but fall back to the raw expression.
  const conflictByExpr = `ON CONFLICT ((lower(brand)), (lower(pn)))`;
  const conflictByName = `ON CONFLICT ON CONSTRAINT ux_${table}_brandpn_expr`;
  const conflict = table ? conflictByName : conflictByExpr;

  const sql = `
    insert into ${qualified} (${insertCols.join(',')})
    values (${params.join(',')})
    ${conflict}
    do update set ${updates.length ? `${updates.join(', ')}, ` : ''}updated_at=now()
    returning *`;

  const res = await db.query(sql, insertVals);
  return res.rows?.[0] || null;
}

module.exports = { ensureSpecsTable, upsertByBrandCode };
