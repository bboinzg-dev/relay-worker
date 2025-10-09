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

module.exports = { upsertByBrandCode };
