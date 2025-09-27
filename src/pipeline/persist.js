'use strict';

const { pool } = require('../utils/db');
const { getColumnsOf } = require('./ensure-spec-columns');

const META_KEYS = new Set([
  'family_slug',
  'brand',
  'brand_norm',
  'pn',
  'pn_norm',
  'code',
  'code_norm',
  'mfr_full',
  'datasheet_uri',
  'verified_in_doc',
  'display_name',
  'displayname',
  'image_uri',
  'cover',
  'series',
  'series_code',
  'raw_json',
  'created_at',
  'updated_at',
]);

const CONFLICT_KEYS = ['brand_norm', 'pn'];

const RANGE_PATTERN = /(-?\d+(?:,\d{3})*(?:\.\d+)?)(?:\s*([kmgmunpµ]))?(?:\s*[a-z%°]*)?\s*(?:to|~|–|—|-)\s*(-?\d+(?:,\d{3})*(?:\.\d+)?)(?:\s*([kmgmunpµ]))?/i;
const NUMBER_PATTERN = /(-?\d+(?:,\d{3})*(?:\.\d+)?)(?:\s*([kmgmunpµ]))?/i;
const SCALE_MAP = {
  k: 1e3,
  m: 1e-3,
  g: 1e9,
  'µ': 1e-6,
  u: 1e-6,
  n: 1e-9,
  p: 1e-12,
};

function normKey(key) {
  return String(key || '')
    .trim()
    .toLowerCase();
}

function isNumericType(type = '') {
  const t = String(type || '').toLowerCase();
  return (
    t.includes('int') ||
    t.includes('numeric') ||
    t.includes('decimal') ||
    t.includes('real') ||
    t.includes('double')
  );
}

function parseNumberToken(token, suffix) {
  if (!token) return null;
  const cleaned = token.replace(/,/g, '');
  const base = Number(cleaned);
  if (!Number.isFinite(base)) return null;
  if (!suffix) return base;
  const scale = SCALE_MAP[suffix.toLowerCase()];
  return scale != null ? base * scale : base;
}

function parseNumericOrRange(value) {
  if (value == null || value === '') return { value: null };
  if (typeof value === 'number') {
    return Number.isFinite(value) ? { value } : { value: null };
  }

  const str = String(value).trim();
  if (!str) return { value: null };

  const rangeMatch = str.match(RANGE_PATTERN);
  if (rangeMatch) {
    const min = parseNumberToken(rangeMatch[1], rangeMatch[2]);
    const max = parseNumberToken(rangeMatch[3], rangeMatch[4]);
    if (min != null && max != null) return { range: { min, max } };
  }

  const singleMatch = str.match(NUMBER_PATTERN);
  if (singleMatch) {
    const parsed = parseNumberToken(singleMatch[1], singleMatch[2]);
    return { value: parsed };
  }

  return { value: null };
}

function deriveRangePrefixes(base) {
  const lower = String(base || '').toLowerCase();
  const prefixes = new Set([lower]);
  const stripped = lower.replace(/_(vdc|vac|v|a|ma|ua|pa|ohm|ω)$/g, '');
  if (stripped && stripped !== lower) prefixes.add(stripped);
  return Array.from(prefixes);
}

function findRangeColumn(columnTypes, base, kind) {
  const prefixes = deriveRangePrefixes(base);
  for (const prefix of prefixes) {
    const re = new RegExp(`^${prefix}_${kind}(?:_[a-z0-9]+)?$`);
    for (const col of columnTypes.keys()) {
      if (re.test(col)) return col;
    }
  }
  return null;
}

async function getColumnTypes(targetTable) {
  const [schema, table] = targetTable.includes('.')
    ? targetTable.split('.', 2)
    : ['public', targetTable];

  const { rows } = await pool.query(
    `SELECT lower(column_name) AS column, data_type
       FROM information_schema.columns
      WHERE table_schema = $1
        AND table_name   = $2`,
    [schema, table],
  );

  const map = new Map();
  for (const row of rows) {
    map.set(row.column, String(row.data_type || '').toLowerCase());
  }
  return map;
}

function coerceScalar(value, type) {
  if (value == null) return null;
  const t = String(type || '').toLowerCase();

  if (!t) return value;

  if (t === 'boolean') {
    const s = String(value).trim().toLowerCase();
    if (/^(true|t|yes|y|1|on)$/i.test(s)) return true;
    if (/^(false|f|no|n|0|off)$/i.test(s)) return false;
    return null;
  }

  if (t.includes('timestamp') || t === 'date') {
    const d = value instanceof Date ? value : new Date(value);
    return Number.isNaN(d.getTime()) ? null : d;
  }

  if (t.includes('json')) {
    if (typeof value === 'string') {
      try {
        return JSON.parse(value);
      } catch (_) {
        return value;
      }
    }
    return value;
  }

  if (Array.isArray(value)) return value;

  const str = String(value).trim();
  return str.length ? str : null;
}

function coerceColumnValue(column, value, columnTypes, record, rawJson, warningSet) {
  const type = columnTypes.get(column);
  if (value == null) return null;

  if (isNumericType(type)) {
    const parsed = parseNumericOrRange(value);
    if (parsed.range) {
      const minCol = findRangeColumn(columnTypes, column, 'min');
      const maxCol = findRangeColumn(columnTypes, column, 'max');
      if (minCol && record[minCol] == null) record[minCol] = parsed.range.min;
      if (maxCol && record[maxCol] == null) record[maxCol] = parsed.range.max;
      if (value != null) rawJson[column] = value;
      warningSet.add('numeric_range_string');
      return null;
    }
    return parsed.value;
  }

  return coerceScalar(value, type);
}

async function loadManufacturerAliasMap() {
  try {
    const { rows } = await pool.query(
      'SELECT brand, alias FROM public.manufacturer_alias',
    );
    const map = new Map();
    for (const { brand, alias } of rows || []) {
      const canonical = String(brand || '').trim();
      const aliasName = String(alias || '').trim();
      if (!canonical) continue;
      map.set(canonical.toLowerCase(), canonical);
      if (aliasName) map.set(aliasName.toLowerCase(), canonical);
    }
    return map;
  } catch (_) {
    return new Map();
  }
}

async function ensureSchemaGuards(familySlug) {
  if (!familySlug) return { ok: true };
  try {
    await pool.query('SELECT public.ensure_specs_table($1)', [familySlug]);
  } catch (err) {
    return { ok: false, reason: 'schema_not_ready', detail: err?.message || String(err) };
  }
  try {
    await pool.query('SELECT public.ensure_blueprint_variant_columns($1)', [familySlug]);
  } catch (err) {
    return { ok: false, reason: 'schema_not_ready', detail: err?.message || String(err) };
  }
  return { ok: true };
}

async function saveExtractedSpecs(targetTable, familySlug, rows = []) {
  const result = { processed: 0, upserts: 0, written: [], skipped: [], warnings: [] };
  if (!rows.length) return result;

  const guard = await ensureSchemaGuards(familySlug);
  if (!guard.ok) {
    result.skipped.push({ reason: guard.reason || 'schema_not_ready', detail: guard.detail || null });
    return result;
  }

  const physicalCols = await getColumnsOf(targetTable);
  if (!physicalCols.size) {
    result.skipped.push({ reason: 'schema_not_ready' });
    return result;
  }

  if (!physicalCols.has('pn') || !physicalCols.has('brand_norm')) {
    result.skipped.push({ reason: 'schema_not_ready' });
    return result;
  }

  const columnTypes = await getColumnTypes(targetTable);
  const aliasMap = await loadManufacturerAliasMap();

  const allKeys = new Set();
  for (const meta of META_KEYS) {
    if (physicalCols.has(meta)) allKeys.add(meta);
  }
  for (const col of columnTypes.keys()) {
    if (/_min(?:_[a-z0-9]+)?$/.test(col) || /_max(?:_[a-z0-9]+)?$/.test(col)) {
      allKeys.add(col);
    }
  }

  for (const row of rows) {
    for (const key of Object.keys(row || {})) {
      const normalized = normKey(key);
      if (physicalCols.has(normalized)) allKeys.add(normalized);
    }
  }

  if (!allKeys.size) {
    result.skipped.push({ reason: 'schema_not_ready' });
    return result;
  }

  const colList = Array.from(allKeys).sort();
  const placeholders = colList.map((_, i) => `$${i + 1}`).join(',');

  const updateCols = colList.filter((col) => !CONFLICT_KEYS.includes(col));
  const updateSql = updateCols.length
    ? updateCols.map((col) => `"${col}" = EXCLUDED."${col}"`).join(', ')
    : null;

  const sql = [
    `INSERT INTO ${targetTable} (${colList.map((c) => `"${c}"`).join(',')})`,
    `VALUES (${placeholders})`,
    'ON CONFLICT (brand_norm, pn)',
    updateSql ? `DO UPDATE SET ${updateSql}` : 'DO NOTHING',
    'RETURNING pn',
  ].join('\n');

  const client = await pool.connect();
  const warnings = new Set();
  const seenNatural = new Set();

  try {
    for (const row of rows) {
      result.processed += 1;
      const rec = {};
      for (const [key, value] of Object.entries(row || {})) {
        rec[normKey(key)] = value;
      }

      const brandRaw = rec.brand ?? rec.brand_norm ?? '';
      const brandTrim = String(brandRaw || '').trim();
      if (!brandTrim || brandTrim.toLowerCase() === 'unknown') {
        result.skipped.push({ reason: 'missing_brand' });
        continue;
      }

      const canonical = aliasMap.get(brandTrim.toLowerCase()) || brandTrim;
      rec.brand = canonical;
      rec.brand_norm = normKey(canonical);
      if (!rec.brand_norm) {
        result.skipped.push({ reason: 'missing_brand' });
        continue;
      }

      const pnRaw = rec.pn ?? rec.code ?? null;
      const pnTrim = typeof pnRaw === 'string' ? pnRaw.trim() : '';
      if (!pnTrim) {
        result.skipped.push({ reason: 'missing_pn' });
        continue;
      }
      rec.pn = pnTrim;
      const pnNorm = normKey(pnTrim);
      if (!pnNorm) {
        result.skipped.push({ reason: 'missing_pn' });
        continue;
      }

      const naturalKey = `${rec.brand_norm}::${pnNorm}`;
      if (seenNatural.has(naturalKey)) {
        result.skipped.push({ reason: 'duplicate_code' });
        continue;
      }
      seenNatural.add(naturalKey);

      if (physicalCols.has('pn_norm')) rec.pn_norm = pnNorm;
      if (rec.code == null) rec.code = pnTrim;
      if (physicalCols.has('code_norm')) rec.code_norm = normKey(rec.code);

      rec.family_slug = familySlug;

      const display = rec.display_name || `${rec.brand} ${pnTrim}`;
      rec.display_name = display;
      if (rec.displayname == null) rec.displayname = display;

      let rawJson = {};
      if (rec.raw_json && typeof rec.raw_json === 'object' && !Array.isArray(rec.raw_json)) {
        rawJson = { ...rec.raw_json };
      } else if (typeof rec.raw_json === 'string') {
        try {
          const parsed = JSON.parse(rec.raw_json);
          if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) rawJson = parsed;
        } catch (_) {}
      }

      const sanitized = {};
      for (const col of colList) {
        if (col === 'raw_json') continue;
        const original = rec[col];
        sanitized[col] = coerceColumnValue(col, original, columnTypes, rec, rawJson, warnings);
      }

      if (physicalCols.has('raw_json')) {
        sanitized.raw_json = Object.keys(rawJson).length ? JSON.stringify(rawJson) : null;
      }

      const vals = colList.map((col) => {
        if (col === 'raw_json') return sanitized.raw_json ?? null;
        return sanitized[col] ?? null;
      });

      try {
        const res = await client.query(sql, vals);
        result.upserts += res.rowCount || 0;
        if (res.rows?.[0]?.pn) result.written.push(res.rows[0].pn);
      } catch (err) {
        result.skipped.push({ reason: 'db_error', detail: err?.message || String(err) });
      }
    }
  } finally {
    client.release();
  }

  result.warnings = Array.from(warnings);
  return result;
}

module.exports = { saveExtractedSpecs };
