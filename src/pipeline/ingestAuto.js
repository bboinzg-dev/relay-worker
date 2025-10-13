'use strict';

const path = require('node:path');
const fs = require('node:fs/promises');
const os = require('node:os');
const crypto = require('node:crypto');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const execFileP = promisify(execFile);

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
const { storage, parseGcsUri, readText, canonicalCoverPath } = require('../utils/gcs');
const { extractText } = require('../utils/extract');
const { getBlueprint } = require('../utils/blueprint');
const { resolveBrand } = require('../utils/brand');
const { detectVariantKeys } = require('../utils/ordering');
const { extractPartsAndSpecsFromPdf } = require('../ai/datasheetExtract');
const { extractFields } = require('./extractByBlueprint');
const { aiCanonicalizeKeys } = require('./ai/canonKeys');
const { saveExtractedSpecs, looksLikeTemplate, renderAnyTemplate } = require('./persist');
const { explodeToRows, splitAndCarryPrefix, normalizeContactForm } = require('../utils/mpn-exploder');
const { extractOrderingRecipe } = require('../utils/vertex');
const {
  ensureSpecColumnsForBlueprint,
  ensureSpecColumnsForKeys,
  getColumnsOf,
} = require('./ensure-spec-columns');
const { ensureSpecsTable } = tryRequire([
  path.join(__dirname, '../utils/schema'),
  path.join(__dirname, '../../utils/schema'),
  path.join(__dirname, '../schema'),
  path.join(process.cwd(), 'schema'),
]);
const { inferVariantKeys, normalizeSlug } = require('./variant-keys');
const { classifyByGcs, extractValuesByGcs } = require('../services/vertex');
const { processDocument: processDocAi } = require('../services/docai');
const { rankPartNumbersFromOrderingSections } = require('../utils/ordering-sections');

const HARD_CAP_MS = Number(process.env.EXTRACT_HARD_CAP_MS || 120000);

const USE_CODE_RULES = /^(1|true|on)$/i.test(process.env.USE_CODE_RULES ?? '1');
const USE_PN_TEMPLATE = /^(1|true|on)$/i.test(process.env.USE_PN_TEMPLATE ?? '1');
const USE_VARIANT_KEYS = /^(1|true|on)$/i.test(
  process.env.USE_VARIANT_KEYS ?? (process.env.USE_CODE_RULES ?? '1')
);
const AUTO_ADD_FIELDS = /^(1|true|on)$/i.test(process.env.AUTO_ADD_FIELDS ?? '1');
const AUTO_ADD_FIELDS_LIMIT_RAW = Number(process.env.AUTO_ADD_FIELDS_LIMIT ?? 20);
const AUTO_ADD_FIELDS_LIMIT = Number.isFinite(AUTO_ADD_FIELDS_LIMIT_RAW)
  ? Math.max(0, AUTO_ADD_FIELDS_LIMIT_RAW)
  : 20;
const VARIANT_MAX_CARDINALITY_INPUT = parseInt(process.env.VARIANT_MAX_CARDINALITY ?? '', 10);
const VARIANT_MAX_CARDINALITY = Number.isFinite(VARIANT_MAX_CARDINALITY_INPUT)
  ? Math.max(2, VARIANT_MAX_CARDINALITY_INPUT)
  : 120;

  // ── Auto-alias learning: unknown spec keys → extraction_recipe.key_alias ──
const AUTO_ALIAS_LEARN = /^(1|true|on)$/i.test(process.env.AUTO_ALIAS_LEARN || '1');
const AUTO_ALIAS_MIN_CONF = Math.max(0, Math.min(1, Number(process.env.AUTO_ALIAS_MIN_CONF || 0.8)));
const AUTO_ALIAS_LIMIT_RAW = Number(process.env.AUTO_ALIAS_LIMIT || 20);
const AUTO_ALIAS_LIMIT = Number.isFinite(AUTO_ALIAS_LIMIT_RAW) ? Math.max(0, AUTO_ALIAS_LIMIT_RAW) : 20;

function withDeadline(promise, ms = HARD_CAP_MS, label = 'op') {
  const timeout = Number.isFinite(ms) && ms > 0 ? ms : HARD_CAP_MS;
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      clearTimeout(timer);
      reject(new Error(`${label}_TIMEOUT`));
    }, timeout);
    Promise.resolve(promise)
      .then((value) => {
        clearTimeout(timer);
        resolve(value);
      })
      .catch((err) => {
        clearTimeout(timer);
        reject(err);
      });
  });
}

const FAST = String(process.env.INGEST_MODE || '').toUpperCase() === 'FAST' || process.env.FAST_INGEST === '1';
const FAST_PAGES = [0, 1, -1]; // 첫 페이지, 2페이지, 마지막 페이지만

const META_KEYS = new Set(['variant_keys','pn_template','ingest_options','_pn_template']);
const BASE_KEYS = new Set([
  'family_slug','brand','code','pn','brand_norm','code_norm','pn_norm','series_code',
  'datasheet_uri','image_uri','datasheet_url','display_name','displayname',
  'cover','verified_in_doc','updated_at'
]);
const SKIP_SPEC_KEYS = new Set([
  'raw_json',
  'text',
  'tables',
  'mpn_list',
  'mpn',
  'codes',
  'series',
  'series_code',
  'raw_text',
  'raw_specs',
  'rawspecs',
  'raw_table',
  'raw_tables',
  'ordering_info',
  'doc_type',
]);

const MERGE_SKIP_KEYS = new Set([
  ...SKIP_SPEC_KEYS,
  'id',
  'created_at',
  'updated_at',
  'family_slug',
  'brand',
  'brand_norm',
  'pn',
  'pn_norm',
  'code',
  'code_norm',
  'display_name',
  'displayname',
  'image_uri',
  'datasheet_uri',
  'cover',
  'verified_in_doc',
  'raw_json',
  'text',
  'tables',
  'mpn',
  'mpn_list',
  'codes',
  'candidates',
  '_doc_text',
  'last_error',
  'run_id',
  'job_id',
  'runid',
  'jobid',
]);

const SPEC_MERGE_OVERRIDES = new Set(['code', 'code_norm', 'pn', 'pn_norm', 'series', 'series_code']);

const DOC_AI_CODE_HEADER_RE =
  /(part\s*(?:no\.?|number|name)|type\s*(?:no\.?|number)?|model|品番|型式|型番|品號|部品番号|품번|형명|주문\s*번호|order(?:ing)?\s*code)/i;

  function gatherRuntimeSpecKeys(rows) {
  const set = new Set();
  const list = Array.isArray(rows) ? rows : [];
  for (const row of list) {
    if (!row || typeof row !== 'object') continue;
    for (const rawKey of Object.keys(row)) {
      const trimmed = String(rawKey || '').trim();
      if (!trimmed) continue;
      const lower = trimmed.toLowerCase();
      if (META_KEYS.has(lower) || BASE_KEYS.has(lower)) continue;
      set.add(trimmed);
    }
  }
  return set;
}

async function ensureDynamicColumnsForRows(qualifiedTable, rows) {
  if (!AUTO_ADD_FIELDS || !AUTO_ADD_FIELDS_LIMIT) return;
  const keys = Array.from(gatherRuntimeSpecKeys(rows)).slice(0, AUTO_ADD_FIELDS_LIMIT);
  if (!keys.length) return;
  const sample = {};
  if (Array.isArray(rows)) {
    const remaining = new Set(keys);
    for (const row of rows) {
      if (!row || typeof row !== 'object') continue;
      for (const key of keys) {
        if (!remaining.has(key)) continue;
        if (Object.prototype.hasOwnProperty.call(row, key)) {
          sample[key] = row[key];
          remaining.delete(key);
        }
      }
      if (!remaining.size) break;
    }
  }
  try {
    await ensureSpecColumnsForKeys(qualifiedTable, keys, sample);
  } catch (err) {
    console.warn('[schema] ensureDynamicColumnsForRows failed:', err?.message || err);
  }
}

function quoteIdentifier(name) {
  const trimmed = String(name || '').trim();
  if (!trimmed) return '""';
  return `"${trimmed.replace(/"/g, '""')}"`;
}

function normalizePnForMerge(value) {
  const raw = String(value || '').toUpperCase().replace(/[^0-9A-Z]/g, '');
  if (raw.length < 4) return null;
  return raw;
}

function getValueIgnoreCase(row, keyLower) {
  if (!row || typeof row !== 'object') return undefined;
  if (Object.prototype.hasOwnProperty.call(row, keyLower)) return row[keyLower];
  const target = String(keyLower || '').toLowerCase();
  for (const [k, v] of Object.entries(row)) {
    if (String(k || '').toLowerCase() === target) return v;
  }
  return undefined;
}

function isEmptyValue(value) {
  if (value == null) return true;
  if (typeof value === 'string') return value.trim() === '';
  if (Array.isArray(value)) return value.length === 0;
  return false;
}

function normalizeDocAiCell(value) {
  return String(value ?? '')
    .normalize('NFKC')
    .replace(/[\u00A0\u2000-\u200B]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function flattenDocAiTablesForMerge(tables) {
  const records = [];
  const list = Array.isArray(tables) ? tables : [];
  for (let tableIndex = 0; tableIndex < list.length; tableIndex += 1) {
    const table = list[tableIndex];
    if (!table || typeof table !== 'object') continue;
    const headers = Array.isArray(table.headers) ? table.headers : [];
    const rows = Array.isArray(table.rows) ? table.rows : [];
    if (!headers.length || !rows.length) continue;
    for (let rowIndex = 0; rowIndex < rows.length; rowIndex += 1) {
      const row = rows[rowIndex];
      if (!Array.isArray(row)) continue;
      const values = {};
      const headerUsage = new Map();
      for (let colIndex = 0; colIndex < headers.length; colIndex += 1) {
        const headerRaw = headers[colIndex];
        let key = normalizeDocAiCell(headerRaw);
        if (!key) key = `column_${colIndex}`;
        const seen = headerUsage.get(key) || 0;
        headerUsage.set(key, seen + 1);
        if (seen > 0) {
          key = `${key}_${seen + 1}`;
        }
        const cellValue = normalizeDocAiCell(row[colIndex]);
        if (!cellValue) continue;
        values[key] = cellValue;
      }
      if (!Object.keys(values).length) continue;
      records.push({
        tableIndex,
        rowIndex,
        headers: headers.map((h) => normalizeDocAiCell(h)),
        values,
      });
    }
  }
  return records;
}

function normalizeDocAiTokenForMatch(value) {
  if (value == null) return null;
  const normalized = String(value)
    .toUpperCase()
    .replace(/[^0-9A-Z]/g, '');
  if (normalized.length < 4) return null;
  return normalized;
}

function bestRowMatchToSpec(row, docAiRecords, used = new Set()) {
  if (!row || typeof row !== 'object') return null;
  const records = Array.isArray(docAiRecords) ? docAiRecords : [];
  if (!records.length) return null;

  const codeKeys = [
    'code',
    'code_norm',
    'pn',
    'pn_norm',
    'part_no',
    'part_number',
    'type_no',
    'type_number',
    'type',
    'typeno',
    'model',
    'model_no',
  ];

  const targetTokens = new Set();
  const rawTargets = new Set();
  for (const key of codeKeys) {
    const value = getValueIgnoreCase(row, key);
    if (value == null || value === '') continue;
    const normalized = normalizeDocAiCell(value);
    if (!normalized) continue;
    rawTargets.add(normalized.toUpperCase());
    const token = normalizeDocAiTokenForMatch(normalized);
    if (token) targetTokens.add(token);
  }

  if (!targetTokens.size) return null;

  let best = null;
  for (const record of records) {
    if (!record || typeof record !== 'object') continue;
    if (used?.has(record)) continue;
    const values = record.values || {};
    const candidateTokens = new Set();
    const haystackParts = [];
    let headerBoost = 0;
    for (const [key, rawValue] of Object.entries(values)) {
      const value = normalizeDocAiCell(rawValue);
      if (!value) continue;
      haystackParts.push(value.toUpperCase());
      const normToken = normalizeDocAiTokenForMatch(value);
      if (normToken) candidateTokens.add(normToken);
      if (DOC_AI_CODE_HEADER_RE.test(String(key || ''))) headerBoost += 1;
    }
    if (!candidateTokens.size) continue;

    let exact = 0;
    let partial = 0;
    for (const token of targetTokens) {
      if (candidateTokens.has(token)) {
        exact += 1;
        continue;
      }
      for (const candidate of candidateTokens) {
        if (candidate.includes(token) || token.includes(candidate)) {
          partial += 1;
          break;
        }
      }
    }
    if (!exact && !partial) continue;

    let score = exact * 12 + partial * 5 + headerBoost;
    if (score <= 0 && rawTargets.size) {
      const haystack = haystackParts.join(' ');
      for (const raw of rawTargets) {
        if (haystack.includes(raw)) score += 2;
      }
    }
    if (score <= 0) continue;

    if (!best || score > best.score) {
      best = { record, score, exact, partial };
    }
  }

  if (!best) return null;
  if (best.exact <= 0 && best.partial <= 0) return null;
  if (best.exact <= 0 && best.score < 12) return null;
  return best.record;
}

function safeMergeSpec(row, source) {
  if (!row || typeof row !== 'object') return {};
  if (!source || typeof source !== 'object') return {};
  const patch = {};
  for (const [rawKey, rawValue] of Object.entries(source)) {
    if (rawKey == null) continue;
    const key = String(rawKey).trim();
    if (!key) continue;
    const lower = key.toLowerCase();
    if (lower.startsWith('_docai')) continue;
    if (rawValue == null) continue;
    if (typeof rawValue === 'string' && rawValue.trim() === '') continue;
    if (MERGE_SKIP_KEYS.has(lower) && !SPEC_MERGE_OVERRIDES.has(lower)) continue;
    const existing = getValueIgnoreCase(row, lower);
    if (!isEmptyValue(existing)) continue;
    patch[key] = rawValue;
  }
  return patch;
}

function normalizeComparableValueForMerge(value) {
  if (value == null) return null;
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) return null;
    return Number(value.toFixed(6));
  }
  if (typeof value === 'boolean') return value ? 1 : 0;
  if (value instanceof Date) return value.getTime();
  if (Array.isArray(value)) {
    const parts = value
      .map((item) => normalizeComparableValueForMerge(item))
      .filter((item) => item != null);
    return parts.length ? parts.join('|') : null;
  }
  const str = String(value).trim();
  if (!str) return null;
  const digitsOnly = str.replace(/,/g, '');
  if (/^-?\d+(?:\.\d+)?$/.test(digitsOnly)) {
    const num = Number(digitsOnly);
    if (Number.isFinite(num)) return Number(num.toFixed(6));
  }
  return str.toLowerCase().replace(/\s+/g, ' ');
}

function valuesConflictForMerge(a, b) {
  const normA = normalizeComparableValueForMerge(a);
  const normB = normalizeComparableValueForMerge(b);
  if (normA == null || normB == null) return false;
  return normA !== normB;
}

function isRecordCompatibleForMerge(record, existing, coreKeySet) {
  const keysToCheck = coreKeySet && coreKeySet.size
    ? Array.from(coreKeySet)
    : Object.keys(record || {}).map((k) => String(k || '').toLowerCase());
  for (const keyLower of keysToCheck) {
    if (!keyLower) continue;
    if (MERGE_SKIP_KEYS.has(keyLower)) continue;
    const recordValue = getValueIgnoreCase(record, keyLower);
    if (isEmptyValue(recordValue)) continue;
    const existingValue = getValueIgnoreCase(existing, keyLower);
    if (isEmptyValue(existingValue)) continue;
    if (valuesConflictForMerge(recordValue, existingValue)) return false;
  }
  return true;
}

function backfillRecordFromExisting(record, existing) {
  if (!record || typeof record !== 'object') return;
  if (!existing || typeof existing !== 'object') return;
  for (const [key, value] of Object.entries(existing)) {
    const lower = String(key || '').toLowerCase();
    if (MERGE_SKIP_KEYS.has(lower)) continue;
    if (value == null) continue;
    if (typeof value === 'object' && !Array.isArray(value)) continue;
    const current = getValueIgnoreCase(record, lower);
    if (!isEmptyValue(current)) continue;
    if (Array.isArray(value) && !value.length) continue;
    record[key] = value;
  }
}

async function mergeRecordsWithExisting({
  records,
  qualifiedTable,
  colTypes,
  brandFallback = null,
  family = null,
  coreKeys = [],
}) {
  if (!Array.isArray(records) || !records.length) return;
  if (!qualifiedTable) return;
  if (!(colTypes instanceof Map)) return;

  const columnSet = new Set();
  for (const key of colTypes.keys()) {
    if (!key) continue;
    columnSet.add(String(key).trim().toLowerCase());
  }

  const comparatorColumns = ['pn_norm', 'pn', 'code_norm', 'code'].filter((col) => columnSet.has(col));
  if (!comparatorColumns.length) return;

  const brandColumn = columnSet.has('brand') ? 'brand' : null;
  if (!brandColumn) return;

  const familyColumn = columnSet.has('family_slug') ? 'family_slug' : null;
  const familyValue = typeof family === 'string' ? family.trim() : '';

  const coreKeySet = new Set(
    Array.isArray(coreKeys)
      ? coreKeys.map((key) => String(key || '').trim().toLowerCase()).filter(Boolean)
      : []
  );

  const cache = new Map();

  const fetchCandidates = async (brandValue, pnNorm) => {
    const cacheKey = `${brandValue.toLowerCase()}::${familyValue.toLowerCase()}::${pnNorm}`;
    if (cache.has(cacheKey)) return cache.get(cacheKey);

    const whereClauses = [];
    const params = [];
    let paramIndex = 1;

    whereClauses.push(`LOWER(${quoteIdentifier(brandColumn)}) = LOWER($${paramIndex})`);
    params.push(brandValue);
    paramIndex += 1;

    if (familyColumn && familyValue) {
      whereClauses.push(`LOWER(${quoteIdentifier(familyColumn)}) = LOWER($${paramIndex})`);
      params.push(familyValue);
      paramIndex += 1;
    }

    const normIndex = paramIndex;
    params.push(pnNorm);

    const comparatorParts = comparatorColumns.map(
      (col) => `regexp_replace(coalesce(${quoteIdentifier(col)}, ''), '[^0-9A-Za-z]', '', 'g') = $${normIndex}`,
    );
    if (!comparatorParts.length) {
      cache.set(cacheKey, []);
      return [];
    }

    whereClauses.push(`(${comparatorParts.join(' OR ')})`);

    const sql = `
      SELECT *
        FROM ${qualifiedTable}
       WHERE ${whereClauses.join(' AND ')}
       ORDER BY updated_at DESC NULLS LAST
       LIMIT 5
    `;

    let rows = [];
    try {
      const { rows: resultRows } = await db.query(sql, params);
      rows = Array.isArray(resultRows) ? resultRows : [];
    } catch (err) {
      console.warn('[merge] existing lookup failed:', err?.message || err);
      rows = [];
    }

    cache.set(cacheKey, rows);
    return rows;
  };

  for (const record of records) {
    if (!record || typeof record !== 'object') continue;
    const brandValue = String(record.brand || brandFallback || '').trim();
    if (!brandValue) continue;
    const pnCandidate = record.pn || record.code;
    const pnNorm = normalizePnForMerge(pnCandidate);
    if (!pnNorm) continue;
    const candidates = await fetchCandidates(brandValue, pnNorm);
    if (!Array.isArray(candidates) || !candidates.length) continue;
    const match = candidates.find((candidate) => isRecordCompatibleForMerge(record, candidate, coreKeySet));
    if (!match) continue;
    backfillRecordFromExisting(record, match);
  }
}

function expandRowsWithVariants(baseRows, options = {}) {
  const list = Array.isArray(baseRows) ? baseRows : [];
  const variantKeys = Array.isArray(options.variantKeys)
    ? Array.from(
        new Set(
          options.variantKeys
            .map((key) => String(key || '').trim())
            .filter(Boolean),
        ),
      )
    : [];
  const pnTemplate = typeof options.pnTemplate === 'string' ? options.pnTemplate : null;
  const defaultBrand = typeof options.defaultBrand === 'string' ? options.defaultBrand : null;
  const defaultSeries = options.defaultSeries ?? null;

  if (!variantKeys.length && !pnTemplate) {
    return list;
  }

  const expanded = [];
  for (const rawRow of list) {
    const baseRow = rawRow && typeof rawRow === 'object' ? { ...rawRow } : {};
    if (defaultBrand) {
      const brandCurrent = String(baseRow.brand || '').trim().toLowerCase();
      if (!brandCurrent || brandCurrent === 'unknown') {
        baseRow.brand = defaultBrand;
      }
    }
    const seriesSeed =
      baseRow.series_code ??
      baseRow.series ??
      (defaultSeries != null ? defaultSeries : null);
    if (seriesSeed != null) {
      if (baseRow.series == null) baseRow.series = seriesSeed;
      if (baseRow.series_code == null) baseRow.series_code = seriesSeed;
    }

    const explodeBase = {
      brand: baseRow.brand ?? defaultBrand ?? null,
      series: baseRow.series ?? seriesSeed ?? null,
      series_code: baseRow.series_code ?? seriesSeed ?? null,
      values: baseRow,
    };

    const haystackSources = [];
    const docText = baseRow._doc_text ?? baseRow.doc_text ?? baseRow.text ?? null;
    if (typeof docText === 'string' && docText.trim()) haystackSources.push(docText);
    const extraText = baseRow.ordering_text ?? baseRow.ordering_snippet ?? null;
    if (typeof extraText === 'string' && extraText.trim()) haystackSources.push(extraText);
    const exploded = explodeToRows(explodeBase, {
      variantKeys,
      pnTemplate,
      haystack: haystackSources,
    }) || [];
    if (Array.isArray(exploded) && exploded.length) {
      for (const item of exploded) {
        if (!item || typeof item !== 'object') continue;
        const values = item.values && typeof item.values === 'object' ? item.values : {};
        const merged = { ...baseRow, ...values };
        if (item.code) merged.code = item.code;
        if (item.code_norm) merged.code_norm = item.code_norm;
        expanded.push(merged);
      }
      continue;
    }

    expanded.push(baseRow);
  }

  return expanded;
}

const PN_CANDIDATE_RE = /[0-9A-Z][0-9A-Z\-_/().#]{3,63}[0-9A-Z)#]/gi;
const PN_BLACKLIST_RE = /(pdf|font|xref|object|type0|ffff)/i;
const PN_STRICT = /^[A-Z0-9][A-Z0-9\-_.()/#]{1,62}[A-Z0-9)#]$/i;

function sanitizeDatasheetUrl(url) {
  if (url == null) return null;
  const str = typeof url === 'string' ? url.trim() : String(url || '').trim();
  if (!str) return null;
  try {
    const parsed = new URL(str);
    const pathname = (parsed.pathname || '').trim();
    if (!pathname) return null;
    const lowerPath = pathname.toLowerCase();
    if (!lowerPath.endsWith('.pdf')) return null;
    return parsed.toString();
  } catch {
    return null;
  }
}

function pickDatasheetUrl(rawUrl, fallbackUrl) {
  const sanitized = sanitizeDatasheetUrl(rawUrl);
  if (sanitized) return sanitized;
  const fallback = typeof fallbackUrl === 'string' ? fallbackUrl.trim() : String(fallbackUrl || '').trim();
  return fallback || null;
}

const RESERVED_SPEC_KEYS = new Set([
  'id',
  'created_at',
  'updated_at',
  'brand',
  'brand_norm',
  'pn',
  'pn_norm',
  'code',
  'series',
  'image_uri',
  'datasheet_uri',
]);

const SPEC_KEY_ALIAS_MAP = new Map([
  ['contact_form', 'contact_arrangement'],
  ['contactform', 'contact_arrangement'],
]);

const TERMINAL_SHAPE_TOKENS = new Set([
  'S',
  'SL',
  'SLF',
  'SLT',
  'SF',
  'SP',
  'ST',
  'SV',
  'SM',
  'P',
  'PC',
  'PD',
  'PY',
  'PT',
  'PR',
  'T',
  'TF',
  'TR',
  'TL',
  'TX',
  'TH',
  'TM',
]);

const PACKING_STYLE_TOKENS = new Set(['Z', 'W', 'X', 'Y']);

function normalizeSpecKeyName(value) {
  if (value == null) return null;
  let s = String(value).trim().toLowerCase();
  if (!s) return null;
  s = s.replace(/[–—―]/g, '-');
  s = s.replace(/\s+/g, '_');
  s = s.replace(/[^0-9a-z_]+/g, '_');
  s = s.replace(/_+/g, '_');
  s = s.replace(/^_|_$/g, '');
  if (!s) return null;
  if (s.length > 63) s = s.slice(0, 63);
  if (RESERVED_SPEC_KEYS.has(s)) return null;
  return s;
}

const ORDERING_SECTION_RE =
  /(ordering information|ordering info|how to order|order information|ordering code|how-to-order|\b品番\b|\b型番\b|\b型号\b|\b型號\b|주문|형명|형번|품번|注文|订购信息|订购|订購|订货|型号)/i;
const CONTACT_LINE_RE = /(contact|arrangement|configuration|form)/i;
const COIL_LINE_RE = /(coil|voltage|vdc)/i;
const CONSTRUCTION_LINE_RE = /(construction|sealed|flux\s*proof|enclosure)/i;
const INSULATION_LINE_RE = /(insulation)/i;
const MATERIAL_LINE_RE = /(material)/i;
const POWER_LINE_RE = /(coil\s*power|power\s*consumption|power\s*code)/i;
const CURRENT_LINE_RE = /(contact\s*current|current\s*\(?type\)?)/i;
const COVER_LINE_RE = /\bcover\b/i;
const TERMINAL_LINE_RE = /(terminal|shape|style)/i;
const PACKING_LINE_RE = /(pack|tape|reel|emboss)/i;
const MOUNT_LINE_RE = /(pc\s*board|surface-?mount|smd)/i;

function normalizeOrderingEnumToken(token) {
  if (token == null) return null;
  const raw = String(token).trim();
  if (!raw) return null;
  if (/^(nil|blank|none|null|n\/a)$/i.test(raw)) return '';
  // 문자 1~2자 또는 숫자 1~2자(예: Cover 1/2)
  if (/^[A-Za-z]{1,2}$/.test(raw)) return raw.toUpperCase();
  if (/^\d{1,2}$/.test(raw)) return raw;
  if (/^(?:[A-Za-z]\d|\d[A-Za-z])$/.test(raw)) return raw.toUpperCase();
  return null;
}

function addOrderingDomainValue(domains, key, value) {
  if (!key) return;
  if (value == null) return;
  const arr = domains.get(key) || [];
  if (!arr.some((item) => item === value)) {
    arr.push(value);
    domains.set(key, arr);
  }
}

function extractContactValues(text) {
  if (!text) return [];
  const out = [];
  const re = /\b((?:\d{1,2}\s*[ABC])+)(?![A-Za-z])/gi;
  let m;
  while ((m = re.exec(text)) !== null) {
    const token = m[1] ? m[1].replace(/\s+/g, '').toUpperCase() : '';
    if (!token) continue;
    if (!/[ABC]/.test(token)) continue;
    out.push(token);
  }
  return out;
}

function extractCoilVoltageValues(text) {
  if (!text) return [];
  const out = [];
  // DC12V / AC220V / 12V / 12 V / 12 VDC / 12 DC / AC 110 / DC 24
  const re = /\b(?:AC|DC)?\s*(\d{1,3})\s*(?:V(?:DC)?|DC)?\b/gi;
  let m;
  while ((m = re.exec(text)) !== null) {
    const num = Number.parseInt(m[1], 10);
    if (!Number.isFinite(num)) continue;
    out.push(`${num} V`);
  }
  // "AC: 6,12,24..." 같이 단위 없는 숫자 나열도 코일/볼티지 맥락이면 허용
  if (!out.length && /(coil|volt|ac|dc)/i.test(text)) {
    for (const match of text.matchAll(/\b(\d{1,3})\b/g)) {
      const n = parseInt(match[1], 10);
      if (Number.isFinite(n)) out.push(`${n} V`);
    }
  }
  return out;
}

// D24/A24, D110/120 + 24D/110/120A + 1H/4H 같은 혼합형도 인식
function extractVoltageCodeTokens(text) {
  if (!text) return [];
  const s = String(text);
  const set = new Set();
  for (const m of s.matchAll(/\b([AD]\d{1,3}(?:\/\d{1,3})?)\b/gi)) set.add(m[1].toUpperCase()); // 접두
  for (const m of s.matchAll(/\b(\d{1,3}(?:\/\d{1,3})?)([AD])\b/gi)) set.add((m[2] + m[1]).toUpperCase()); // 접미
  for (const m of s.matchAll(/\b(\d{1,2}[Hh])\b/g)) set.add(m[1].toUpperCase());
  return [...set];
}

// '1 Form C' → '1C'
function extractContactFormsFromLine(text) {
  if (!text) return [];
  const out = [];
  for (const m of String(text).matchAll(/(\d)\s*form\s*([ABC])/gi)) out.push(`${m[1]}${m[2].toUpperCase()}`);
  return out;
}

function extractEnumCodeValues(text) {
  if (!text) return [];
  const out = [];
  const hay = String(text);
  const directRe = /(Nil|Blank|None|[A-Za-z]{1,3})\s*(?=[:=（(\-])/g;
  let m;
  while ((m = directRe.exec(hay)) !== null) {
    const normalized = normalizeOrderingEnumToken(m[1]);
    if (normalized == null) continue;
    out.push(normalized);
  }
  for (const qm of hay.matchAll(/["'’”]([A-Za-z0-9]{1,3})["'’”]/g)) {
    const token = normalizeOrderingEnumToken(qm[1]);
    if (token != null) out.push(token);
  }
  if (out.length) return out;

  const fragments = hay
    .split(/[\n,\/|•·]+/)
    .map((part) => part.replace(/[()]/g, '').trim())
    .filter(Boolean);
  for (const frag of fragments) {
    const normalized = normalizeOrderingEnumToken(frag);
    if (normalized == null) continue;
    out.push(normalized);
  }
  return out;
}

function gatherOrderingTexts(info, acc = []) {
  if (!info) return acc;
  if (typeof info === 'string') {
    const trimmed = info.trim();
    if (!trimmed) return acc;
    try {
      const parsed = JSON.parse(trimmed);
      return gatherOrderingTexts(parsed, acc);
    } catch (_) {
      acc.push(trimmed);
      return acc;
    }
  }
  if (Array.isArray(info)) {
    for (const entry of info) gatherOrderingTexts(entry, acc);
    return acc;
  }
  if (typeof info !== 'object') return acc;

  const candidates = [
    info.text,
    info.window_text,
    info.windowText,
    info.context,
    info.snippet,
    info.preview,
  ];
  for (const candidate of candidates) {
    if (typeof candidate === 'string' && candidate.trim()) {
      acc.push(candidate.trim());
    }
  }
  if (info.window && typeof info.window === 'object') {
    gatherOrderingTexts(info.window, acc);
  }
  if (Array.isArray(info.sections)) {
    for (const section of info.sections) gatherOrderingTexts(section, acc);
  }
  return acc;
}

function sliceOrderingSections(text) {
  if (!text) return [];
  const normalized = String(text).replace(/\r/g, '\n');
  const sections = [];
  const re = new RegExp(ORDERING_SECTION_RE.source, 'gi');
  let match;
  while ((match = re.exec(normalized)) !== null) {
    const start = Math.max(0, match.index - 80);
    const end = Math.min(normalized.length, match.index + 1200);
    sections.push(normalized.slice(start, end));
  }
  if (!sections.length && normalized.trim()) {
    sections.push(normalized.slice(0, 1200));
  }
  return sections;
}

function collectOrderingDomains({ orderingInfo, previewText, docAiText, docAiTables }) {
  const domains = new Map();

  const addMany = (key, values) => {
    if (!Array.isArray(values)) return;
    for (const value of values) {
      if (value == null) continue;
      addOrderingDomainValue(domains, key, value);
    }
  };

  if (Array.isArray(docAiTables)) {
    for (const table of docAiTables) {
      if (!table || typeof table !== 'object') continue;
      const headers = Array.isArray(table.headers) ? table.headers : [];
      const rows = Array.isArray(table.rows) ? table.rows : [];
      if (!headers.length || !rows.length) continue;
      const keyByIndex = headers.map((header) => {
        const norm = String(header || '').trim().toLowerCase();
        if (!norm) return null;
        if (/contact/.test(norm) || /arrangement/.test(norm) || /configuration/.test(norm)) return 'contact_arrangement';
        if (/coil/.test(norm) && /volt/.test(norm)) return 'coil_voltage_vdc';
        if (/voltage\s*\(vdc\)/.test(norm)) return 'coil_voltage_vdc';
        if (/coil/.test(norm) && /power/.test(norm)) return 'coil_power_code';
        if (/power/.test(norm) && /code/.test(norm)) return 'coil_power_code';
        if (/(terminal|shape|style)/.test(norm)) return 'terminal_shape';
        if (/(pack|tape|reel|emboss)/.test(norm)) return 'packing_style';
        if (/(pc\s*board|surface-?mount|smd)/.test(norm)) return 'mount_type';
        if (/contact/.test(norm) && /current/.test(norm)) return 'contact_current_code';
        if (/cover/.test(norm)) return 'cover_code';
        if (/construction/.test(norm) || /enclosure/.test(norm)) return 'construction';
        if (/insulation/.test(norm)) return 'insulation_code';
        if (/material/.test(norm)) return 'material_code';
        return null;
      });
      if (!keyByIndex.some(Boolean)) continue;
      for (const row of rows) {
        if (!Array.isArray(row)) continue;
        row.forEach((cell, idx) => {
          const key = keyByIndex[idx];
          if (!key) return;
          const text = typeof cell === 'string' ? cell : String(cell ?? '');
          if (!text.trim()) return;
          if (key === 'contact_arrangement') addMany(key, extractContactValues(text));
          else if (key === 'coil_voltage_vdc') addMany(key, extractCoilVoltageValues(text));
          else addMany(key, extractEnumCodeValues(text));
        });
      }
    }
  }

  const textSources = new Set();
  gatherOrderingTexts(orderingInfo, []).forEach((txt) => textSources.add(txt));
  if (typeof docAiText === 'string' && docAiText.trim()) textSources.add(docAiText);
  if (typeof previewText === 'string' && previewText.trim()) textSources.add(previewText);

  for (const rawText of textSources) {
    const sections = sliceOrderingSections(rawText); // 주문/형명/품번 중심
    const typesWin = (() => {
      const full = String(rawText || '');
      const idx = full.search(/\bTYPES\b/i);
      if (idx >= 0) return full.slice(Math.max(0, idx - 4000), Math.min(full.length, idx + 16000));
      return '';
    })();
    if (typesWin) sections.push(typesWin);
    for (const section of sections) {
      const lines = section.split(/\n+/);
      for (const rawLine of lines) {
        const line = rawLine.replace(/^[\s•·\-–—]+/, '').trim();
        if (!line) continue;
        if (CONTACT_LINE_RE.test(line)) {
          addMany('contact_arrangement', extractContactValues(line));
          addMany('contact_arrangement', extractContactFormsFromLine(line));
        }
        if (COIL_LINE_RE.test(line)) {
          addMany('coil_voltage_vdc', extractCoilVoltageValues(line));
          addMany('coil_voltage_code', extractVoltageCodeTokens(section));
        }
        if (CONSTRUCTION_LINE_RE.test(line)) addMany('construction', extractEnumCodeValues(line));
        if (INSULATION_LINE_RE.test(line)) addMany('insulation_code', extractEnumCodeValues(line));
        if (MATERIAL_LINE_RE.test(line)) addMany('material_code', extractEnumCodeValues(line));
        if (POWER_LINE_RE.test(line)) addMany('coil_power_code', extractEnumCodeValues(line));
        if (CURRENT_LINE_RE.test(line)) addMany('contact_current_code', extractEnumCodeValues(line));
        if (TERMINAL_LINE_RE.test(line)) addMany('terminal_shape', extractEnumCodeValues(line));
        if (PACKING_LINE_RE.test(line)) addMany('packing_style', extractEnumCodeValues(line));
        if (MOUNT_LINE_RE.test(line)) addMany('mount_type', extractEnumCodeValues(line));
        if (COVER_LINE_RE.test(line)) addMany('cover_code', extractEnumCodeValues(line));
        // 자주 나오는 일반 패턴들
        if (/led/i.test(line)) addMany('led_code', extractEnumCodeValues(line)); // "L: With LED, Nil: W/O LED"
        if (/A\s*type\s*:\s*A/i.test(line) && /S\s*type\s*:\s*S/i.test(line)) {
          addMany('terminal_shape', ['A', 'S']);
        }
        if (/tape.*reel.*pack/i.test(line)) addMany('packing_style', ['Z', 'X', 'W', 'Y']);
        if (/tube\s*pack/i.test(line)) addMany('packing_style', ['TUBE', 'NIL']);
        if (/cover/i.test(line)) {
          const modes = Array.from(line.matchAll(/\b([12])\b/g)).map((m) => m[1]);
          if (modes.length) addMany('cover_mode', modes);
        }
        if (/(insert|pcb)/i.test(line)) {
          const types = Array.from(line.matchAll(/\b([ab])\s*[:=]/gi)).map((m) => m[1].toLowerCase());
          if (types.length) addMany('mount_type_code', types);
        }
      }
    }
  }

  const result = {};
  for (const [key, values] of domains.entries()) {
    if (!values || !values.length) continue;
    result[key] = values;
  }
  const normalizedDomains = Object.keys(result).length ? result : null;
  return {
    domains: normalizedDomains,
    textSources: Array.from(textSources),
  };
}

function buildTyOrderingFallback({ baseSeries, orderingInfo, previewText, docAiText }) {
  const normalizedSeries = String(baseSeries || '').trim().toUpperCase();
  if (normalizedSeries && normalizedSeries !== 'TY') return null;

  const textSources = new Set();
  gatherOrderingTexts(orderingInfo, []).forEach((txt) => textSources.add(txt));
  if (typeof docAiText === 'string' && docAiText.trim()) textSources.add(docAiText);
  if (typeof previewText === 'string' && previewText.trim()) textSources.add(previewText);

  if (!textSources.size) return null;
  const haystack = Array.from(textSources).join('\n').toUpperCase();
  if (!haystack) return null;
  if (!/(ORDERING|HOW TO ORDER|주문|注文)/.test(haystack)) return null;

  const hasTyMarker = /\bTY(?:\b|[-\d])/i.test(haystack);
  if (!normalizedSeries && !hasTyMarker) return null;

  const fallbackSeries = normalizedSeries || 'TY';
  return {
    domains: {
      contact_arrangement: ['1C'],
      coil_voltage_vdc: ['1.5 V', '2.4 V', '3 V', '4.5 V', '5 V', '6 V', '9 V', '12 V', '24 V'],
      construction: ['S'],
      coil_power_code: ['', 'H'],
    },
    pnTemplate: '{{series}}-{{coil_voltage_vdc|digits}}{{construction}}{{coil_power_code}}',
    series: fallbackSeries,
  };
}

function extractOrderingTemplate(orderingInfo) {
  if (!orderingInfo) return null;
  if (typeof orderingInfo === 'string') {
    const trimmed = orderingInfo.trim();
    if (!trimmed) return null;
    try {
      const parsed = JSON.parse(trimmed);
      return extractOrderingTemplate(parsed);
    } catch (_) {
      return null;
    }
  }
  if (Array.isArray(orderingInfo)) {
    for (const entry of orderingInfo) {
      const tpl = extractOrderingTemplate(entry);
      if (tpl) return tpl;
    }
    return null;
  }
  if (typeof orderingInfo !== 'object') return null;

  const candidates = [
    orderingInfo.pn_template,
    orderingInfo.pnTemplate,
    orderingInfo.template,
    orderingInfo.mpn_template,
    orderingInfo.code_template,
  ];
  for (const candidate of candidates) {
    if (typeof candidate === 'string' && candidate.trim()) {
      return candidate.trim();
    }
  }
  if (orderingInfo.window && typeof orderingInfo.window === 'object') {
    const tpl = extractOrderingTemplate(orderingInfo.window);
    if (tpl) return tpl;
  }
  if (Array.isArray(orderingInfo.sections)) {
    for (const section of orderingInfo.sections) {
      const tpl = extractOrderingTemplate(section);
      if (tpl) return tpl;
    }
  }
  return null;
}

function escapeRegex(str) {
  return String(str || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function textContainsExact(text, pn) {
  if (!text || !pn) return false;
  const pattern = escapeRegex(String(pn).trim());
  if (!pattern) return false;
  const re = new RegExp(`(^|[^A-Za-z0-9])${pattern}(?=$|[^A-Za-z0-9])`, 'i');
  return re.test(String(text));
}

function normLower(s){ return String(s||'').trim().toLowerCase(); }

function isValidCode(s) {
  const v = String(s || '').trim();
  if (!v) return false;
  if (v.length < 2 || v.length > 64) return false;
  if (!/[0-9A-Za-z]/.test(v)) return false;
  if (/\s{2,}/.test(v)) return false;
  if (/^pdf-?1(\.\d+)?$/i.test(v)) return false;
  return true;
}

const KEY_ALIASES = {
  form: ['form', 'contact_form', 'contact_arrangement', 'configuration', 'arrangement', 'poles_form'],
  voltage: ['voltage', 'coil_voltage_code', 'coil_voltage_vdc', 'voltage_vdc', 'rated_voltage_vdc', 'vdc', 'coil_voltage'],
  case: ['case', 'case_code', 'package', 'pkg'],
  led: ['led', 'led_code', 'indicator'],
  cover: ['cover', 'cover_mode', 'cover_code'],
  mount: ['mount', 'mount_type', 'mount_type_code', 'insert_type', 'assembly'],
  capacitance: ['capacitance', 'capacitance_uF', 'capacitance_f', 'c'],
  resistance: ['resistance', 'resistance_ohm', 'r_ohm', 'r'],
  tolerance: ['tolerance', 'tolerance_pct'],
  length_mm: ['length_mm', 'dim_l_mm'],
  width_mm: ['width_mm', 'dim_w_mm'],
  height_mm: ['height_mm', 'dim_h_mm'],
  series: ['series', 'series_code'],
  terminal: ['terminal', 'terminal_form', 'terminal_form_code', 'terminal_code'],
};

const ENUM_MAP = {
  form: { SPST: '1A', SPDT: '1C', DPDT: '2C' }
};

function normalizeKeyName(v) {
  return String(v || '')
    .normalize('NFKC')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '');
}
function normalizeAlias(v) {
  return String(v || '')
    .normalize('NFKC')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function mergeVariantKeyLists(limit, ...lists) {
  const max = Number.isFinite(limit) && limit > 0 ? limit : VARIANT_MAX_CARDINALITY;
  const seen = new Set();
  const out = [];
  for (const list of lists) {
    if (!Array.isArray(list)) continue;
    for (const rawKey of list) {
      const norm = String(rawKey || '').trim().toLowerCase();
      if (!norm) continue;
      if (seen.has(norm)) continue;
      seen.add(norm);
      out.push(norm);
      if (out.length >= max) return out;
    }
  }
  return out;
}

// extraction_recipe.recipe.key_alias { canonical_key: [ alias... ] } upsert
async function upsertRecipeKeyAliases({ family, brand, series, additions }) {
  if (!family || !additions || typeof additions !== 'object') return { updated: false };
  const brandSlug = normalizeSlug(brand); // already imported from variant-keys
  const seriesSlug = normalizeSlug(series);
  const sel = await db.query(
    `SELECT recipe FROM public.extraction_recipe
      WHERE family_slug=$1 AND COALESCE(brand_slug,'')=COALESCE($2,'') AND COALESCE(series_slug,'')=COALESCE($3,'')
      LIMIT 1`,
    [family, brandSlug, seriesSlug]
  ); /* uses the same table variant-keys reads from */ /* :contentReference[oaicite:5]{index=5} */
  let recipe = sel.rows[0]?.recipe;
  if (typeof recipe === 'string') {
    try {
      recipe = JSON.parse(recipe);
    } catch {
      recipe = {};
    }
  }
  if (!recipe || typeof recipe !== 'object') recipe = {};
  const keyAlias = recipe.key_alias && typeof recipe.key_alias === 'object' ? { ...recipe.key_alias } : {};
  let changed = 0;
  for (const [key, arr] of Object.entries(additions)) {
    const k = normalizeKeyName(key);
    if (!k) continue;
    const exists = new Set(Array.isArray(keyAlias[k]) ? keyAlias[k].map(normalizeAlias) : []);
    const incoming = (Array.isArray(arr) ? arr : [arr]).map(normalizeAlias).filter(Boolean);
    const merged = [...new Set([...exists, ...incoming])];
    if (!merged.length) continue;
    keyAlias[k] = merged;
    changed += merged.length - exists.size;
  }
  if (!changed) return { updated: false };
  recipe.key_alias = keyAlias;
  if (sel.rows.length) {
    await db.query(
      `UPDATE public.extraction_recipe
         SET recipe=$4
       WHERE family_slug=$1 AND COALESCE(brand_slug,'')=COALESCE($2,'') AND COALESCE(series_slug,'')=COALESCE($3,'')`,
      [family, brandSlug, seriesSlug, JSON.stringify(recipe)]
    );
  } else {
    await db.query(
      `INSERT INTO public.extraction_recipe (family_slug, brand_slug, series_slug, recipe)
       VALUES ($1,$2,$3,$4)`,
      [family, brandSlug, seriesSlug, JSON.stringify(recipe)]
    );
  }
  return { updated: true, added: changed };
}

function collectUnknownKeysFromExtraction(extracted, knownSet) {
  const out = new Set();
  const rows = Array.isArray(extracted?.rows) ? extracted.rows : [];
  for (const row of rows) {
    if (!row || typeof row !== 'object') continue;
    for (const raw of Object.keys(row)) {
      const norm = normalizeKeyName(raw);
      if (!norm) continue;
      if (knownSet?.has?.(norm)) continue;
      out.add(raw);
    }
  }
  return Array.from(out);
}

async function learnAndPersistAliases({ family, brand, series, blueprint, extracted }) {
  if (!AUTO_ALIAS_LEARN) return { added: 0 };
  // blueprint.allowedKeys / variant_keys = known 키 집합
  const known = new Set(
    [].concat(
      Array.isArray(blueprint?.allowedKeys) ? blueprint.allowedKeys.map(normalizeKeyName) : [],
      Array.isArray(blueprint?.variant_keys) ? blueprint.variant_keys.map(normalizeKeyName) : []
    )
  );
  const candidates = collectUnknownKeysFromExtraction(extracted, known).slice(0, AUTO_ALIAS_LIMIT * 2);
  if (!candidates.length) return { added: 0 };
  const { map } = await aiCanonicalizeKeys(family, candidates, Array.from(known)); /* :contentReference[oaicite:6]{index=6} */
  const additions = {};
  let added = 0;
  for (const raw of candidates) {
    const rec = map?.[raw];
    if (!rec || rec.action !== 'map') continue;
    if (!Number.isFinite(rec.conf) || rec.conf < AUTO_ALIAS_MIN_CONF) continue;
    const canonical = normalizeKeyName(rec.canonical);
    if (!canonical || !known.has(canonical)) continue;
    (additions[canonical] ||= []).push(raw);
    added += 1;
    if (added >= AUTO_ALIAS_LIMIT) break;
  }
  if (!added) return { added: 0 };
  await upsertRecipeKeyAliases({ family, brand, series, additions });
  return { added };
}
function pickField(rec, aliases = []) {
  if (!rec || typeof rec !== 'object') return null;
  for (const key of aliases) {
    const raw = rec?.[key];
    if (raw == null) continue;
    if (Array.isArray(raw)) {
      const first = raw.find((v) => v != null && String(v).trim() !== '');
      if (first != null) return first;
      continue;
    }
    if (typeof raw === 'object') {
      if ('value' in raw && raw.value != null && String(raw.value).trim() !== '') {
        return raw.value;
      }
      continue;
    }
    const trimmed = String(raw).trim();
    if (trimmed !== '') return raw;
  }
  return null;
}

function applyOps(val, ops) {
  let current = Array.isArray(val) ? val[0] : val;
  let s = current == null ? '' : String(current);
  for (const rawOp of ops) {
    if (!rawOp) continue;
    const opToken = rawOp.includes('=') ? rawOp.replace('=', ':') : rawOp;
    const op = opToken.trim();
    const lower = op.toLowerCase();
    if (lower === 'first') {
      s = s.split(',')[0].trim();
      continue;
    }
    if (lower === 'upper') {
      s = s.toUpperCase();
      continue;
    }
    if (lower === 'alnum') {
      s = s.replace(/[^0-9A-Z]/gi, '');
      continue;
    }
    if (lower === 'digits') {
      const digits = s.match(/\d+/g) || [''];
      s = digits.join('');
      continue;
    }
    if (lower === 'num') {
      const match = s.match(/-?\d+(?:\.\d+)?/);
      s = match ? match[0] : '';
      continue;
    }
    if (lower.startsWith('pad:')) {
      const [, widthRaw] = op.split(':');
      const width = Number(widthRaw) || 2;
      s = s.padStart(width, '0');
      continue;
    }
    if (lower.startsWith('slice:')) {
      const parts = op.split(':');
      const start = Number(parts[1]) || 0;
      const end = parts.length > 2 && parts[2] !== '' ? Number(parts[2]) : undefined;
      s = s.slice(start, Number.isNaN(end) ? undefined : end);
      continue;
    }
    if (lower.startsWith('map:')) {
      const mapPairs = op.slice(4).split(',');
      const mapping = Object.create(null);
      for (const pair of mapPairs) {
        const [from, to] = pair.split('>');
        if (!from || to == null) continue;
        mapping[String(from).trim().toUpperCase()] = String(to).trim();
      }
      const key = String(s).trim().toUpperCase();
      s = mapping[key] ?? s;
      continue;
    }
  }
  return s;
}

function resolveToken(base, rec, ctxText = '') {
  const aliases = KEY_ALIASES[base] || [base];
  let value = pickField(rec, aliases);

  if (base === 'form') {
    if (!value) {
      value = pickField(rec, ['contact_arrangement', 'configuration']);
    }

    if (value) {
      const norm = normalizeContactForm(value);
      if (norm) value = norm;
    }

    if (!value && ctxText) {
      const m = ctxText.match(/(\d)\s*form\s*([ABC])/i) || ctxText.match(/\b(SPST|SPDT|DPDT)\b/i);
      if (m) {
        const norm = normalizeContactForm(m[0]);
        if (norm) value = norm;
      }
    }
  }

  if (!value && base === 'case' && ctxText) {
    const match = ctxText.match(/\b(SOT-?23|TO-220|DFN\d+x\d+)\b/i);
    if (match) value = match[1];
  }

  if (value) {
    const enumMap = ENUM_MAP[base];
    if (enumMap) {
      const normalized = String(value).trim().toUpperCase();
      value = enumMap[normalized] ?? enumMap[normalized.replace(/\s+/g, '')] ?? value;
    }
  }

  return value ?? '';
}

function renderTemplate(tpl, rec, ctxText = '') {
  if (!tpl) return '';
  const input = String(tpl);
  const rendered = input.replace(/\{([^}]+)\}/g, (_, body) => {
    const parts = body.split('|').map((part) => part.trim()).filter(Boolean);
    if (!parts.length) return '';
    const base = (parts.shift() || '').toLowerCase();
    const raw = resolveToken(base, rec, ctxText);
    return applyOps(raw, parts);
  });
  return rendered.replace(/\s+/g, '').trim();
}

function renderFromTemplate(rec, pnTemplate, variantKeys = []) {
  if (!pnTemplate) return null;
  const ctxText = rec?._doc_text || rec?.doc_text || '';
  const advanced = renderTemplate(pnTemplate, rec, ctxText);
  // 텍스트 존재 검증: 문서 본문에 실제로 나타나는 코드만 허용
  if (advanced && textContainsExact(ctxText, advanced)) return advanced;

  let out = pnTemplate;
  const dict = new Map(Object.entries(rec || {}));
  for (const k of variantKeys) if (!dict.has(k)) dict.set(k, rec?.[k]);
  dict.set('series', rec?.series ?? rec?.series_code ?? '');
  dict.set('series_code', rec?.series_code ?? rec?.series ?? '');
  out = out.replace(/\{([a-z0-9_]+)\}/ig, (_, k) => String(dict.get(k) ?? ''));
  out = out.replace(/\s+/g,'').trim();
  return out && textContainsExact(ctxText, out) ? out : null;
}

function recoverCode(rec, { pnTemplate, variantKeys }) {
  let c = rec.code || rec.pn || null;
  if (isValidCode(c)) return c;

  const fromTpl = renderFromTemplate(rec, pnTemplate, variantKeys);
  if (isValidCode(fromTpl)) return fromTpl;

  const parts = [];
  if (rec.series_code || rec.series) parts.push(rec.series_code || rec.series);
  for (const k of (Array.isArray(variantKeys) ? variantKeys : [])) {
    if (rec[k] != null) parts.push(String(rec[k]));
  }
  const guess = parts.join('');
  if (isValidCode(guess)) return guess;

  if (Array.isArray(rec.candidates) && rec.candidates.length) {
    const first = String(rec.candidates[0] || '').trim();
    if (isValidCode(first)) return first;
  }
  return null;
}

function renderInlineTemplate(value, context) {
  if (!looksLikeTemplate(value)) return value;
  const rendered = renderAnyTemplate(value, context);
  return rendered != null && rendered !== '' ? rendered : null;
}

function sanitizeRecordTemplates(records) {
  if (!Array.isArray(records)) return;
  for (const rec of records) {
    if (!rec || typeof rec !== 'object') continue;
    const context = { ...rec };
    const renderedPn = renderInlineTemplate(context.pn, context);
    if (looksLikeTemplate(context.pn)) {
      rec.pn = renderedPn ?? null;
    } else if (renderedPn != null) {
      rec.pn = renderedPn;
    }

    const contextForCode = { ...context, pn: rec.pn ?? context.pn };
    const renderedCode = renderInlineTemplate(context.code, contextForCode);
    if (looksLikeTemplate(context.code)) {
      rec.code = renderedCode ?? null;
    } else if (renderedCode != null) {
      rec.code = renderedCode;
    }

    if (!rec.pn && rec.code) {
      rec.pn = rec.code;
    }
  }
}

function pickBrandHint(...values) {
  for (const value of values) {
    if (value == null) continue;
    const trimmed = String(value).trim();
    if (!trimmed) continue;
    if (trimmed.toLowerCase() === 'unknown') continue;
    return trimmed;
  }
  return null;
}

function mergeRuntimeMetadata(rawJson, meta = {}) {
  const hasBrandSource = Object.prototype.hasOwnProperty.call(meta, 'brand_source');
  const hasVariantKeys = Object.prototype.hasOwnProperty.call(meta, 'variant_keys_runtime');
  if (!hasBrandSource && !hasVariantKeys) return rawJson;

  let base = {};
  if (rawJson && typeof rawJson === 'object' && !Array.isArray(rawJson)) {
    base = { ...rawJson };
  } else if (typeof rawJson === 'string') {
    try {
      const parsed = JSON.parse(rawJson);
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
        base = parsed;
      }
    } catch (_) {
      base = {};
    }
  } else if (rawJson != null) {
    base = { value: rawJson };
  }

  if (hasBrandSource) base.brand_source = meta.brand_source ?? null;
  if (hasVariantKeys) base.variant_keys_runtime = meta.variant_keys_runtime ?? [];
  return base;
}

// --- 브랜드 자동 감지 (manufacturer_alias 기반) ---
async function detectBrandFromText(text = '', fileName = '') {
  const hay = `${String(fileName || '')} ${String(text || '')}`.toLowerCase();
  if (!hay.trim()) return null;
  try {
    const { rows } = await db.query(
      `SELECT brand, brand_norm, alias, aliases FROM public.manufacturer_alias`
    );
    for (const row of rows) {
      if (!row) continue;
      const tokens = new Set();
      if (row.brand) tokens.add(String(row.brand));
      if (row.brand_norm) tokens.add(String(row.brand_norm));
      if (row.alias) tokens.add(String(row.alias));
      if (Array.isArray(row.aliases)) {
        for (const a of row.aliases) tokens.add(String(a));
      } else if (typeof row.aliases === 'string') {
        tokens.add(row.aliases);
      }
      for (const token of tokens) {
        const trimmed = String(token || '').trim();
        if (!trimmed) continue;
        if (trimmed.toLowerCase() === 'unknown') continue;
        if (trimmed.length < 2) continue;
        const pattern = escapeRegex(trimmed.toLowerCase());
        if (!pattern) continue;
        const re = new RegExp(`(^|[^a-z0-9])${pattern}([^a-z0-9]|$)`, 'i');
        if (re.test(hay)) return String(row.brand || trimmed).trim();
      }
    }
  } catch (err) {
    console.warn('[brand detect] failed:', err?.message || err);
  }
  return null;
}

function harvestMpnCandidates(text, series){
  const hay = String(text || '');
  if (!hay) return [];
  const ser = String(series || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
  const seen = new Set();
  const out = [];
  PN_CANDIDATE_RE.lastIndex = 0;
  let match;
  while ((match = PN_CANDIDATE_RE.exec(hay)) != null) {
    const raw = match[0];
    if (!raw) continue;
    if (PN_BLACKLIST_RE.test(raw)) continue;
    const norm = raw.toUpperCase();
    // 접두 '정확히 시작'만 허용 → '포함'까지 허용 (예: series=TQ, 코드=ATQ2S-5V-Z)
    if (ser && norm && !(norm.startsWith(ser) || norm.includes(ser))) continue;
    if (seen.has(norm)) continue;
    seen.add(norm);
    out.push(raw.trim());
  }
  return out;
}

// DB 컬럼 타입 조회 (fallback용)
async function getColumnTypes(qualified) {
  const [schema, table] = qualified.includes('.') ? qualified.split('.') : ['public', qualified];
  const q = `
    SELECT lower(column_name) AS col, lower(data_type) AS dt
    FROM information_schema.columns
    WHERE table_schema=$1 AND table_name=$2`;
  const { rows } = await db.query(q, [schema, table]);
  const out = new Map();
  for (const { col, dt } of rows) {
    if (/(integer|bigint|smallint)/.test(dt)) out.set(col, 'int');
    else if (/(numeric|decimal|double precision|real)/.test(dt)) out.set(col, 'numeric');
    else if (/boolean/.test(dt)) out.set(col, 'bool');
    else out.set(col, 'text');
  }
  return out;
}

// 숫자 강제정규화(콤마/단위/리스트/범위 허용 → 첫 숫자만)
function coerceNumeric(x) {
  if (x == null || x === '') return null;
  if (typeof x === 'number') return x;
  let s = String(x).toLowerCase().replace(/(?<=\d),(?=\d{3}\b)/g, '').replace(/\s+/g, ' ').trim();
  if (/-?\d+(?:\.\d+)?\s*(?:to|~|–|—|-)\s*-?\d+(?:\.\d+)?/.test(s)) return null;
  const m = s.match(/(-?\d+(?:\.\d+)?)(?:\s*([kmgmunpµ]))?/i);
  if (!m) return null;
  let n = parseFloat(m[1]);
  const mul = (m[2] || '').toLowerCase();
  const scale = { k:1e3, m:1e-3, 'µ':1e-6, u:1e-6, n:1e-9, p:1e-12, g:1e9 };
  if (mul && scale[mul] != null) n *= scale[mul];
  return Number.isFinite(n) ? n : null;
}

function toInt(v){
  if (v==null || v==='') return null;
  const s = String(v).replace(/[^0-9\-]/g,'');
  const n = parseInt(s, 10);
  return Number.isFinite(n) ? n : null;
}

function toBool(v){
  const s = String(v ?? '').trim().toLowerCase();
  if (!s) return null;
  if (/^(true|1|y|yes|on)$/i.test(s))  return true;
  if (/^(false|0|n|no|off)$/i.test(s)) return false;
  return null;
}

function normTableText(value) {
  return String(value || '')
    .normalize('NFKC')
    .replace(/[–—−]/g, '-')
    .replace(/\s+/g, ' ')
    .trim();
}

function pickSkuListFromTables(extracted = {}) {
  const tables = Array.isArray(extracted.tables) ? extracted.tables : [];
  if (!tables.length) return [];

  const PN_HEADER = /(part\s*(?:no\.?|number)|type\s*(?:no\.?|number)?|catalog\s*(?:no\.?|number)|model|品番|型式|형명|주문\s*번호|order(?:ing)?\s*code)/i;
  const TABLE_HINT = /(ordering|part\s*number|type\s*number|catalog|selection|list\s*of\s*types|品番|型式|형명)/i;

  const set = new Set();
  for (const table of tables) {
    if (!table || typeof table !== 'object') continue;
    const headers = Array.isArray(table.headers) ? table.headers : [];
    if (!headers.length) continue;
    const headerNorms = headers.map((h) => normTableText(h));
    if (!headerNorms.length) continue;
    const headerText = headerNorms.join(' ');
    if (!TABLE_HINT.test(headerText) && !headerNorms.some((h) => PN_HEADER.test(h))) continue;

    const pnIndexes = headerNorms
      .map((h, idx) => (PN_HEADER.test(h) ? idx : -1))
      .filter((idx) => idx >= 0);
    if (!pnIndexes.length) continue;

    for (const row of Array.isArray(table.rows) ? table.rows : []) {
      if (!Array.isArray(row)) continue;
      for (const idx of pnIndexes) {
        const cell = normTableText(row[idx]);
        if (!cell) continue;
        PN_CANDIDATE_RE.lastIndex = 0;
        let m;
        while ((m = PN_CANDIDATE_RE.exec(cell)) != null) {
          const raw = m[0];
          if (!raw) continue;
          if (PN_BLACKLIST_RE.test(raw)) continue;
          set.add(raw.trim());
        }
      }
    }
  }

  return Array.from(set);
}

function expandFromCodeSystem(extracted, bp, docText = '') {
  const tpl = bp?.fields?.code_template;
  const vars = bp?.fields?.code_vars;
  if (!tpl || !vars) return [];
  const haystack = String(docText || '');
  if (!haystack.trim()) return [];

  const keys = Object.keys(vars);
  const out = new Set();
  const MAX_EXPANSION = 400;
  function dfs(i, ctx) {
    if (i >= keys.length) {
      let code = tpl;
      for (const k of Object.keys(ctx)) {
        const v = ctx[k];
        code = code.replace(new RegExp(`\\{${k}(:[^}]*)?\\}`, 'g'), (_, fmt) => {
          if (!fmt) return String(v);
          const m = fmt.match(/^:0(\d+)d$/);
          if (m) return String(v).padStart(Number(m[1]), '0');
          return String(v);
        });
      }
      const cleaned = String(code || '').trim();
      if (!cleaned) return;
      if (!textContainsExact(haystack, cleaned)) return;
      if (PN_BLACKLIST_RE.test(cleaned)) return;
      out.add(cleaned);
      return;
    }
    const k = keys[i];
    const list = Array.isArray(vars[k]) ? vars[k] : [];
    for (const v of list) {
      if (out.size >= MAX_EXPANSION) break;
      dfs(i + 1, { ...ctx, [k]: v });
    }
  }
  dfs(0, {});
  return Array.from(out).slice(0, MAX_EXPANSION);
}

function splitCodeSegments(code) {
  return String(code || '')
    .toUpperCase()
    .split(/[-_/\\\s]+/)
    .map((segment) => segment.trim())
    .filter((segment) => segment.length > 0);
}

function detectTerminalShapeFromCode(code) {
  const segments = splitCodeSegments(code);
  for (const segment of segments) {
    if (!/^[A-Z]{1,4}$/.test(segment)) continue;
    if (TERMINAL_SHAPE_TOKENS.has(segment)) return segment;
    for (const token of TERMINAL_SHAPE_TOKENS) {
      if (segment.startsWith(token) && segment.length <= token.length + 1) return token;
    }
  }
  const normalized = String(code || '').toUpperCase();
  const match = normalized.match(/(?:-|\\)([A-Z]{1,3})(?=\d{0,3}(?:$|[^A-Z0-9]))/);
  if (match && TERMINAL_SHAPE_TOKENS.has(match[1])) return match[1];
  return null;
}

function detectPackingStyleFromCode(code) {
  const normalized = String(code || '').toUpperCase();
  const direct = normalized.match(/(?:-|\\)([ZWXY])(?=[^A-Z0-9]|$)/);
  if (direct && PACKING_STYLE_TOKENS.has(direct[1])) return direct[1];
  const segments = splitCodeSegments(code);
  for (const segment of segments) {
    if (segment.length === 1 && PACKING_STYLE_TOKENS.has(segment)) return segment;
    if (/^[0-9]*[ZWXY]$/.test(segment)) return segment.slice(-1);
  }
  const lastChar = normalized.replace(/[^A-Z0-9]+$/g, '').slice(-1);
  if (PACKING_STYLE_TOKENS.has(lastChar)) return lastChar;
  return null;
}

function detectOperatingFunctionFromCode(code) {
  const segments = splitCodeSegments(code);
  for (const segment of segments) {
    if (/^L2[A-Z0-9]*$/.test(segment)) return 'dual_coil_latching';
  }
  for (const segment of segments) {
    if (/^L[A-Z0-9]*$/.test(segment) && segment.length <= 3) return 'latching';
  }
  for (const segment of segments) {
    if (/^D[A-Z0-9]*$/.test(segment) && segment.length <= 3) return 'dual_coil';
  }
  return null;
}

function applyDefaultCodeHeuristics(code, out, colTypes) {
  if (!code || !out || typeof out !== 'object') return;
  if (!(colTypes instanceof Map)) return;
  const normalized = String(code || '').toUpperCase();

  const terminalShape = detectTerminalShapeFromCode(code);
  if (
    terminalShape &&
    colTypes.has('terminal_shape') &&
    (out.terminal_shape == null || out.terminal_shape === '')
  ) {
    out.terminal_shape = terminalShape;
  }

  const packingStyle = detectPackingStyleFromCode(code);
  if (
    packingStyle &&
    colTypes.has('packing_style') &&
    (out.packing_style == null || out.packing_style === '')
  ) {
    out.packing_style = packingStyle;
  }

  const operating = detectOperatingFunctionFromCode(code);
  if (
    operating &&
    colTypes.has('operating_function') &&
    (out.operating_function == null || out.operating_function === '')
  ) {
    out.operating_function = operating;
  }
  if (operating && /latching/i.test(operating)) {
    if (colTypes.has('is_latching') && (out.is_latching == null || out.is_latching === '')) {
      out.is_latching = true;
    }
  }

  const mbbDetected = /(^|[^A-Z0-9])MBB([^A-Z0-9]|$)/.test(normalized);
  if (mbbDetected) {
    if (colTypes.has('mbb') && (out.mbb == null || out.mbb === '')) {
      out.mbb = true;
    }
    if (colTypes.has('is_mbb') && (out.is_mbb == null || out.is_mbb === '')) {
      out.is_mbb = true;
    }
  }
}

function applyCodeRules(code, out, rules, colTypes) {
  const columnTypes = colTypes instanceof Map ? colTypes : new Map();
  const ruleList = Array.isArray(rules) ? rules : [];
  const src = String(code || '');
  for (const r of ruleList) {
    const re = new RegExp(r.pattern, r.flags || 'i');
    const m = src.match(re);
    if (!m) continue;
    for (const [col, spec] of Object.entries(r.set || {})) {
      if (!columnTypes.has(col)) continue;
      let v;
      const gname = spec.from || '1';
      v = (m.groups && m.groups[gname]) || m[gname] || m[1] || null;
      if (v == null) continue;
      if (spec.map) v = spec.map[v] ?? v;
      if (spec.numeric) v = coerceNumeric(v);
      if (v == null || v === '') continue;
      out[col] = v;
    }
  }
    applyDefaultCodeHeuristics(code, out, columnTypes);
}


// DB 함수로 스키마 보장 (ensure_specs_table)
async function ensureSpecsTableByFamily(family, qualified){
  if (!family) return;
  try {
    await db.query(`SELECT public.ensure_specs_table($1)`, [family]);
    return;
  } catch (err) {
    console.warn('[schema] ensure_specs_table failed:', err?.message || err);
    if (qualified) {
      try {
        await ensureSpecsTable(qualified);
        return;
      } catch (fallbackErr) {
        console.warn('[schema] local ensureSpecsTable fallback failed:', fallbackErr?.message || fallbackErr);
      }
    }
    throw err;
  }
}

async function ensureBlueprintVariantColumns(family) {
  await db.query(`SELECT public.ensure_blueprint_variant_columns($1)`, [family]);
}

async function extractCoverToGcs(gcsPdfUri, { family, brand, code }) {
  try {
    const { bucket, name } = parseGcsUri(gcsPdfUri);
    const tmp = path.join(os.tmpdir(), 'pdf-'+Date.now());
    const pdf = path.join(tmp, 'doc.pdf');
    await fs.mkdir(tmp, { recursive: true });
    const [buf] = await storage.bucket(bucket).file(name).download();
    await fs.writeFile(pdf, buf);

    // 일부 PDF에서 pdfimages가 매우 오래 걸리거나 멈추는 사례 방지
    await execFileP('pdfimages', ['-f','1','-l','2','-png', pdf, path.join(tmp,'img')], {
      timeout: Number(process.env.COVER_EXTRACT_TIMEOUT_MS || 45000), // 45s
      maxBuffer: 16 * 1024 * 1024,
    });
    const list = (await fs.readdir(tmp)).filter(f => /^img-\d+-\d+\.png$/i.test(f));
    if (!list.length) return null;
    let pick=null, size=-1;
    for (const f of list) {
      const st = await fs.stat(path.join(tmp, f));
      if (st.size > size) { pick=f; size=st.size; }
    }
    if (!pick) return null;

    const dst = canonicalCoverPath((process.env.ASSET_BUCKET || process.env.GCS_BUCKET || '').replace(/^gs:\/\//,''), family, brand, code);
    const { bucket: outBkt, name: outName } = parseGcsUri(dst);
    await storage.bucket(outBkt).upload(path.join(tmp, pick), { destination: outName, resumable:false });
    return dst;
  } catch { return null; }
}

function guessFamilySlug({ fileName = '', previewText = '', brand = '' }) {
  const haystack = [fileName, previewText, brand]
    .filter((part) => part != null && String(part).trim() !== '')
    .join(' ')
    .toLowerCase();

  if (!haystack) return null;

  const hasRelay = /\brelay\b/.test(haystack);
  const hasSignal = /\bsignal\s+relay\b/.test(haystack) ||
    /\bsubminiature\b.*\brelay\b/.test(haystack) ||
    /\btelecom\b.*\brelay\b/.test(haystack) ||
    /\bminiature\s+relay\b/.test(haystack);

  if (hasSignal || /\bty\b(?![a-z0-9])/i.test(haystack)) {
    return 'relay_signal';
  }

  if (/\breed\b.*\brelay\b/.test(haystack)) {
    return 'relay_reed';
  }

  if (/\b(ssr|solid\s+state)\b.*\brelay\b/.test(haystack)) {
    return 'relay_ssr';
  }

  if (/\b(automotive|vehicle|car)\b.*\brelay\b/.test(haystack)) {
    return 'relay_automotive';
  }

  if (hasRelay) {
    const signalBrand = /\b(axicom|takamisawa|nec\s*tokin|fujitsu)\b/.test(haystack);
    if (signalBrand) {
      return 'relay_signal';
    }

    const powerHints = /\b(power\s+relay|general\s+purpose\s+relay|high\s+power|power\s+load|heavy\s+duty)\b/;
    if (powerHints.test(haystack)) {
      return 'relay_power';
    }
  }

  if (/\b(resistor|r-clamp|ohm)\b/.test(haystack)) return 'resistor_chip';
  if (/\b(capacitor|mlcc|electrolytic|tantalum)\b/.test(haystack)) return 'capacitor_mlcc';
  if (/\b(inductor|choke)\b/.test(haystack)) return 'inductor_power';
  if (/\b(bridge|rectifier|diode)\b/.test(haystack)) return 'bridge_rectifier';

  return null;
}

function normalizeCode(str) {
  return String(str || '')
    .replace(/[–—]/g, '-')      // 유니코드 대시 정규화
    .replace(/\s+/g, '')        // 내부 공백 제거
    .replace(/-+/g, '-')        // 대시 연속 정리
    .toUpperCase();
}

// --- 문서 타입 감지: 단일 / 카탈로그 / 오더링 섹션 ---
function resolveDocTypeFromExtraction(payload, text = '', signals = {}) {
  if (!payload || typeof payload !== 'object') return null;

  const existingRaw = typeof payload.doc_type === 'string' ? payload.doc_type.trim().toLowerCase() : '';
  const orderingCodes = Array.isArray(payload?.ordering_info?.codes)
    ? payload.ordering_info.codes.filter(Boolean).length
    : 0;

      const vertexHintRaw = typeof signals?.vertexDocType === 'string'
    ? signals.vertexDocType.trim().toLowerCase()
    : '';
  const docTypeHintRaw = typeof signals?.docTypeHint === 'string'
    ? signals.docTypeHint.trim().toLowerCase()
    : '';
  const orderingHits = Array.isArray(signals?.orderingHits) ? signals.orderingHits : [];
  const orderingHitCount = Number.isFinite(signals?.orderingHitCount)
    ? Number(signals.orderingHitCount)
    : orderingHits.length;
  const orderingScore = Number.isFinite(signals?.orderingScore)
    ? Number(signals.orderingScore)
    : orderingHits.reduce((sum, item) => sum + Number(item?.score || 0), 0);
  const orderingInfoCodes = Array.isArray(signals?.orderingInfo?.codes)
    ? signals.orderingInfo.codes.filter(Boolean).length
    : 0;
  const orderingSignal = Math.max(orderingCodes, orderingInfoCodes, orderingHitCount);
  const hasOrderingRecipe = Boolean(signals?.hasOrderingRecipe);

  const rowCodes = new Set();
  if (Array.isArray(payload.rows)) {
    for (const row of payload.rows) {
      if (!row || typeof row !== 'object') continue;
      const code = String(row.code || row.pn || '').trim().toUpperCase();
      if (code) rowCodes.add(code);
    }
  }

  let candidateList = [];
  if (Array.isArray(payload.codes) && payload.codes.length) {
    candidateList = payload.codes;
  } else if (Array.isArray(payload.mpn_list) && payload.mpn_list.length) {
    candidateList = payload.mpn_list;
  }
  const candidateCodes = new Set(
    candidateList.map((code) => String(code || '').trim().toUpperCase()).filter(Boolean)
  );

  const haystack = String(text || '').toLowerCase();

  const orderingKeywordHit = Boolean(
    haystack && (
      haystack.includes('how to order') ||
      haystack.includes('ordering information') ||
      haystack.includes('ordering info') ||
      haystack.includes('주문') ||
      haystack.includes('订购') ||
      haystack.includes('订货')
    )
  );

  const catalogKeywordHit = Boolean(
    haystack && (
      haystack.includes('catalog') ||
      haystack.includes('product list') ||
      haystack.includes('types') ||
      haystack.includes('part no') ||
      haystack.includes('part number')
    )
  );

  const baseHint = docTypeHintRaw || vertexHintRaw || '';

  const strongOrdering =
    orderingSignal > 0 ||
    orderingKeywordHit ||
    hasOrderingRecipe ||
    orderingScore >= 6 ||
    (orderingHitCount >= 2 && candidateCodes.size > 1);

  let inferred = baseHint || 'single';
  if (inferred !== 'ordering') {
    if (strongOrdering) {
      inferred = 'ordering';
    } else if (inferred !== 'catalog') {
      if (rowCodes.size > 1 || candidateCodes.size > 1 || catalogKeywordHit) {
        inferred = 'catalog';
      } else {
        inferred = 'single';
      }
    }
  }

  if (inferred === 'catalog' && strongOrdering) {
    inferred = 'ordering';
  }

  if (existingRaw === 'ordering') return 'ordering';
  if (existingRaw === 'catalog') {
    return strongOrdering ? 'ordering' : 'catalog';
  }
  if (existingRaw === 'single') {
    if (inferred === 'ordering') return 'ordering';
    if (inferred === 'catalog') return 'catalog';
    return 'single';
  }

  if (strongOrdering) return 'ordering';
  if (candidateCodes.size > 10 || rowCodes.size > 10) return 'catalog';
  if (candidateCodes.size > 1) return 'catalog';
  if (rowCodes.size > 1) return 'catalog';

  if (orderingCodes > 0 || orderingInfoCodes > 0 || orderingHitCount > 0) return 'ordering';

  if (orderingKeywordHit) return 'ordering';
  if (catalogKeywordHit) return 'catalog';
  if (haystack.includes('catalog')) return 'catalog';
  if (haystack.includes('series information')) return 'catalog';
  if (haystack.includes('ordering guide')) return 'ordering';
  if (haystack.includes('ordering information')) return 'ordering';
  if (haystack.includes('ordering info')) return 'ordering';

  return inferred;
}

// ---- NEW: "TYPES / Part No." 표에서 품번 열거 추출 ----
function _expandAorS(code) {
  return code.includes('*') ? [code.replace('*','A'), code.replace('*','S')] : [code];
}
function _looksLikePn(s) {
  const c = s.toUpperCase();
  if (!/[0-9]/.test(c)) return false;
  if (c.length < 4 || c.length > 24) return false;
  // 명백한 단위/잡토큰 제거
  if (/^(ISO|ROHS|VDC|VAC|V|A|MA|MM|Ω|OHM|PDF|PAGE|NOTE|DATE|LOT|WWW|HTTP|HTTPS)$/i.test(c)) return false;
  return true;
}
function extractPartNumbersFromTypesTables(full, limit = 200) {
  const text = String(full || '');
  if (!text) return [];
  // TYPES, Part No. 주변 10~16KB 윈도우로 좁힌다
  const idxTypes = text.search(/\bTYPES\b/i);
  const idxPart  = text.search(/\bPart\s*No\.?\b/i);
  const anchor   = (idxTypes >= 0 ? idxTypes : 0);
  const start    = Math.max(0, Math.min(anchor, idxPart >= 0 ? idxPart : anchor) - 4000);
  const end      = Math.min(text.length, (anchor || 0) + 16000);
  const win      = text.slice(start, end);

  // Part No. 패턴: 대문자+숫자 혼합(하이픈/별표 허용)
  const raw = win.match(/[A-Z][A-Z0-9][A-Z0-9\-\*]{2,}/g) || [];

  // “* = A/S” 치환 규칙 감지(있으면 확장)
  const hasStarRule = /"\s*\*\s*"\s*:.*A\s*type\s*:\s*A.*S\s*type\s*:\s*S/i.test(win);

  const set = new Set();
  for (const r of raw) {
    const code = normalizeCode(r);
    if (!_looksLikePn(code)) continue;
    const list = hasStarRule ? _expandAorS(code) : [code];
    for (const c of list) set.add(c);
  }
  return Array.from(set).slice(0, limit).map(c => ({ code: c }));
}

// --- NEW: 표를 못 찾을 때를 위한 "시리즈 접두 기반" 보조 휴리스틱 ---
function extractPartNumbersBySeriesHeuristic(full, limit = 200) {
  const text = String(full || '');
  if (!text) return [];
  // 1) 문서 전체에서 "문자 2~5개 + 숫자 시작" 패턴으로 접두 후보 수집
  const seed = text.match(/[A-Z]{2,5}(?=\d)/g) || [];
  const freq = new Map();
  for (const p of seed) freq.set(p, (freq.get(p) || 0) + 1);
  const tops = [...freq.entries()].sort((a, b) => b[1] - a[1]).slice(0, 3).map(([p]) => p);
  if (!tops.length) return [];
  // 2) 각 접두에 대해 PN 후보 수집
  const set = new Set();
  for (const pref of tops) {
    const re = new RegExp(`${pref}[A-Z0-9*\\-]{3,}`, 'g');
    const raw = text.toUpperCase().match(re) || [];
    for (const candidate of raw) {
      // 숫자/길이/노이즈 필터
      if (!/[0-9]/.test(candidate)) continue;
      if (candidate.length < 4 || candidate.length > 24) continue;
      if (/^(ISO|ROHS|VDC|VAC|V|A|MA|MM|Ω|OHM|PDF|PAGE|NOTE|DATE|LOT|WWW|HTTP|HTTPS)$/.test(candidate)) continue;
      // A/S 확장
      if (candidate.includes('*')) {
        set.add(candidate.replace('*', 'A'));
        set.add(candidate.replace('*', 'S'));
      } else {
        set.add(candidate);
      }
      if (set.size >= limit) break;
    }
    if (set.size >= limit) break;
  }
  return [...set].slice(0, limit).map(code => ({ code }));
}



// 품번 후보 추출 (ordering/types/series 휴리스틱 재사용)
async function extractPartNumbersFromText(text, { series } = {}) {
  const src = String(text || '');
  if (!src) return [];

  const prefix = series ? normalizeCode(series) : null;
  const seen = new Set();
  const out = [];

  const push = (raw) => {
    if (!raw) return;
    const norm = normalizeCode(raw);
    if (!norm) return;
    if (prefix && !norm.startsWith(prefix)) return;
    if (seen.has(norm)) return;
    seen.add(norm);
    const cleaned = typeof raw === 'string' ? raw.trim() : String(raw || '');
    out.push(cleaned || norm);
  };

  for (const { code } of extractPartNumbersFromTypesTables(src, 200)) push(code);
  for (const { code } of rankPartNumbersFromOrderingSections(src, 200)) push(code);
  for (const { code } of extractPartNumbersBySeriesHeuristic(src, 200)) push(code);

  return out;
}



async function doIngestPipeline(input = {}, runIdParam = null) {
  let {
    gcsUri: rawGcsUri = null,
    gsUri: rawGsUri = null,
    family_slug = null,
    brand = null,
    code = null,
    series = null,
    display_name = null,
  } = input;

  const overridesBrand = input?.overrides?.brand ?? null;
  const overridesSeries = input?.overrides?.series ?? null;
  const effectiveBrand = overridesBrand || brand || null;
  let detectedBrand = null;
  if (overridesSeries != null && (series == null || series === '')) series = overridesSeries;

  const gcsUri = (rawGcsUri || rawGsUri || '').trim();
  const runId = runIdParam ?? input?.runId ?? input?.run_id ?? null;
  const jobId = input?.jobId ?? input?.job_id ?? null;

  const started = Date.now();
  if (!gcsUri) throw new Error('gcsUri/gsUri required');
  // 기본 2분 하드캡 (ENV로 조정 가능)
  const BUDGET = Number(process.env.INGEST_BUDGET_MS || 120000);
  const FAST = /^(1|true|on)$/i.test(process.env.FAST_INGEST || '1');
  const PREVIEW_BYTES = Number(process.env.PREVIEW_BYTES || 262144);
  const EXTRACT_HARD_CAP_MS = HARD_CAP_MS;
  const FIRST_PASS_CODES = parseInt(process.env.FIRST_PASS_CODES || '20', 10);

  let lockAcquired = false;
  if (runId) {
    try {
      await db.query('SELECT pg_advisory_lock(hashtext($1))', [runId]);
      lockAcquired = true;
    } catch (err) {
      console.warn('[ingest] advisory lock failed:', err?.message || err);
    }
  }

  const releaseLock = async () => {
    if (!lockAcquired || !runId) return;
    lockAcquired = false;
    try {
      await db.query('SELECT pg_advisory_unlock(hashtext($1))', [runId]);
    } catch (err) {
      console.warn('[ingest] advisory unlock failed:', err?.message || err);
    }
  };

  const withTimeout = (p, ms, label) => new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`TIMEOUT:${label}`)), ms);
    Promise.resolve(p)
      .then((val) => { clearTimeout(timer); resolve(val); })
      .catch((err) => { clearTimeout(timer); reject(err); });
  });

  let docAiResult = null;
  let vertexClassification = null;
  let vertexExtractValues = null;

  const runnerPromise = (async () => {

    const ensureExtractedShape = (value) => {
      const base = value && typeof value === 'object' ? value : {};
      if (!Array.isArray(base.tables)) base.tables = [];
      if (!Array.isArray(base.rows)) base.rows = [];
      if (typeof base.text !== 'string') base.text = '';
      return base;
    };

    const mergeExtracted = (source) => {
      if (!source || typeof source !== 'object') return;
      if (typeof source.text === 'string') {
        extracted.text = String(source.text);
      }
      if (Array.isArray(source.tables)) {
        extracted.tables = source.tables;
      }
      if (Array.isArray(source.rows)) {
        extracted.rows = source.rows;
      }
      for (const [key, value] of Object.entries(source)) {
        if (key === 'text' || key === 'tables' || key === 'rows') continue;
        extracted[key] = value;
      }
      ensureExtractedShape(extracted);
    };

    let extracted = ensureExtractedShape(input?.extracted);
    let docExtractResult = null;

    // family 추정 (미지정 시 일부 텍스트만 읽어 빠르게 추정)
  let fileName = '';
  try { const { name } = parseGcsUri(gcsUri); fileName = path.basename(name); } catch {}

  if (!docAiResult) {
    try {
      docAiResult = await withDeadline(
        processDocAi(gcsUri, { runId }),
        HARD_CAP_MS,
        'DOCAI_PROCESS',
      );
    } catch (err) {
      console.warn('[docai] process failed:', err?.message || err);
    }
  }

  if (!vertexClassification) {
    try {
      vertexClassification = await classifyByGcs(gcsUri, fileName || 'datasheet.pdf');
    } catch (err) {
      console.warn('[vertex] classify failed:', err?.message || err);
    }
  }

  if (!family_slug && vertexClassification?.family_slug) {
    family_slug = vertexClassification.family_slug;
  }
  if (!overridesBrand && !brand && vertexClassification?.brand) {
    brand = vertexClassification.brand;
  }
  if (!detectedBrand && vertexClassification?.brand) {
    detectedBrand = vertexClassification.brand;
  }
  if (!code && vertexClassification?.code) {
    code = vertexClassification.code;
  }
  if (!series && vertexClassification?.series) {
    series = vertexClassification.series;
  }

  const explicitFamily = normLower(family_slug);
  const vertexFamily = normLower(vertexClassification?.family_slug);
  const brandGuessInput = overridesBrand || brand || detectedBrand || vertexClassification?.brand || '';
  const initialGuess = guessFamilySlug({ fileName, brand: brandGuessInput });

  let previewText = '';
  let docAiText = typeof docAiResult?.text === 'string' ? docAiResult.text : '';
  let docAiTables = Array.isArray(docAiResult?.tables) ? docAiResult.tables : [];

  if (!FAST) {
    try {
      previewText = await readText(gcsUri, 256 * 1024);
    } catch {}
  }

  if ((!previewText || previewText.length < 1000) && !FAST) {
    try {
      docExtractResult = await extractText(gcsUri);
      previewText = docExtractResult?.text || previewText;
      extracted.text = String(docExtractResult?.text || extracted.text || '');
    } catch {}
  }

  if (docAiText && docAiText.length > (previewText?.length || 0)) {
    previewText = docAiText;
  }

  let family = explicitFamily || vertexFamily || initialGuess || null;
  const previewGuess = guessFamilySlug({ fileName, previewText, brand: brandGuessInput });

  if (!explicitFamily) {
    if (previewGuess && (!family || family === 'relay_power' || family === vertexFamily)) {
      family = previewGuess;
    }
    if ((!family || family === 'relay_power') && /subminiature\s+signal\s+relay|signal\s+relay/i.test(previewText)) {
      family = 'relay_signal';
    }
  }

  if (!family) family = 'relay_power';

  const overrideBrandLog = overridesBrand ?? brand ?? '';
  console.log(`[PATH] overrides.brand=${overrideBrandLog || ''} family=${family} runId=${runId || ''}`);

// 목적 테이블
  const reg = await db.query(
    `SELECT specs_table FROM public.component_registry WHERE family_slug=$1 LIMIT 1`,
    [family]
  );
  const table = reg.rows[0]?.specs_table || 'relay_power_specs';
  const qualified = table.startsWith('public.')? table : `public.${table}`;

  let blueprint = await getBlueprint(family);

  if (!vertexExtractValues && family) {
    try {
      vertexExtractValues = await extractValuesByGcs(gcsUri, family);
    } catch (err) {
      console.warn('[vertex] extract failed:', err?.message || err);
    }
  }

  // 블루프린트 허용 키
  let allowedKeys = Array.isArray(blueprint?.allowedKeys)
    ? [...blueprint.allowedKeys]
    : [];
  if ((!allowedKeys || !allowedKeys.length) && blueprint?.fields && typeof blueprint.fields === 'object') {
    allowedKeys = Object.keys(blueprint.fields);
  }
  allowedKeys = Array.from(
    new Set(
      (allowedKeys || [])
        .map((k) => String(k || '').trim())
        .filter(Boolean)
    )
  );

  let variantKeys = [];
  if (USE_VARIANT_KEYS) {
    variantKeys = Array.isArray(blueprint?.ingestOptions?.variant_keys)
      ? blueprint.ingestOptions.variant_keys
      : (Array.isArray(blueprint?.variant_keys) ? blueprint.variant_keys : []);
    variantKeys = variantKeys
      .map((k) => String(k || '').trim().toLowerCase())
      .filter(Boolean);
  }

  let pnTemplate = USE_PN_TEMPLATE
    ? (blueprint?.ingestOptions?.pn_template || blueprint?.ingestOptions?.pnTemplate || null)
    : null;
  const requiredFields = [];
  if (blueprint?.fields && typeof blueprint.fields === 'object') {
    for (const [fieldKey, meta] of Object.entries(blueprint.fields)) {
      const isRequired = meta && typeof meta === 'object' && Boolean(meta.required);
      if (!isRequired) continue;
      const normalized = String(fieldKey || '')
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9_]/g, '');
      if (normalized && !requiredFields.includes(normalized)) {
        requiredFields.push(normalized);
      }
    }
  }

  let colTypes;
  const disableEnsure = /^(1|true|on)$/i.test(process.env.NO_SCHEMA_ENSURE || '0');

  // -------- 공용 강제정규화 유틸 --------

  if (code && !/\d/.test(String(code))) {
    // "AGN","TQ" 처럼 숫자 없는 시리즈는 series로 넘기고 code는 비움
    series = code; code = null;
  }


  // ❶ PDF 텍스트 일부에서 품번 후보 우선 확보
  if (!previewText && !FAST) {
    try { previewText = await readText(gcsUri, PREVIEW_BYTES) || ''; } catch {}
  }
  if ((!previewText || previewText.length < 1000) && !FAST) {
    try {
      docExtractResult = await extractText(gcsUri);
      previewText = docExtractResult?.text || previewText;
      extracted.text = String(docExtractResult?.text || extracted.text || '');
    } catch {}
  }
  if (!docAiText) {
    docAiText = typeof docAiResult?.text === 'string' ? docAiResult.text : '';
  }
  if (!Array.isArray(docAiTables) || !docAiTables.length) {
    docAiTables = Array.isArray(docAiResult?.tables) ? docAiResult.tables : [];
  }
  if (docAiText && docAiText.length > (previewText?.length || 0)) {
    previewText = docAiText;
  }
  if (!effectiveBrand) {
    try {
      detectedBrand = await detectBrandFromText(previewText, fileName);
    } catch (err) {
      console.warn('[brand detect] preview failed:', err?.message || err);
    }
  }
  let candidates = [];
  try {
    candidates = await extractPartNumbersFromText(previewText, { series: series || code });
  } catch { candidates = []; }

  // PDF → 품번/스펙 추출
  const brandHint = effectiveBrand || detectedBrand || null;
  extracted.brand = extracted.brand || brandHint || 'unknown';
  if (!effectiveBrand || !code) {
    try {
      if (FAST) {
        // 텍스트만 빠르게 읽어 블루프린트 기반 추출
        let raw = previewText;
        if (!raw) {
          try { raw = await readText(gcsUri, PREVIEW_BYTES); } catch { raw = ''; }
        }
        if (raw && raw.length > 1000) {
          const fieldsJson = blueprint?.fields || {};
          const vals = await extractFields(raw, code || '', fieldsJson);
          const fallbackBrand = brandHint || 'unknown';
          extracted.brand = extracted.brand || fallbackBrand;
          extracted.rows = [
            {
              brand: fallbackBrand,
              code: code || path.parse(fileName).name,
              ...(vals || {}),
            },
          ];
        } else {
          // 스캔/이미지형 PDF 등 텍스트가 없으면 정밀 추출을 1회만 하드캡으로 시도
          const pdfExtract = await withTimeout(
            extractPartsAndSpecsFromPdf({ gcsUri, allowedKeys, family, brandHint }),
            EXTRACT_HARD_CAP_MS,
            'extract',
          );
          mergeExtracted(pdfExtract);
        }
      } else {
        const pdfExtract = await withTimeout(
          extractPartsAndSpecsFromPdf({ gcsUri, allowedKeys, family, brandHint }),
          EXTRACT_HARD_CAP_MS,
          'extract',
        );
        mergeExtracted(pdfExtract);
      }
    } catch (e) { console.warn('[extract timeout/fail]', e?.message || e); }
  }

  if (docAiText) {
    const existing = typeof extracted.text === 'string' ? extracted.text : '';
    if (!existing || docAiText.length > existing.length) {
      extracted.text = docAiText;
    }
  }
  if (docAiTables.length && !extracted.tables.length) {
    extracted.tables = docAiTables;
  }

  const docAiRecordsForMerge = flattenDocAiTablesForMerge(docAiTables);
  const vertexSpecValues = (() => {
    if (!vertexExtractValues || typeof vertexExtractValues !== 'object') return null;
    const filtered = {};
    for (const [rawKey, rawValue] of Object.entries(vertexExtractValues)) {
      const key = String(rawKey || '').trim();
      if (!key) continue;
      if (rawValue == null) continue;
      if (typeof rawValue === 'string' && rawValue.trim() === '') continue;
      filtered[key] = rawValue;
    }
    return Object.keys(filtered).length ? filtered : null;
  })();

  ensureExtractedShape(extracted);

  const usedDocAiRecords = new Set();
  if (!Array.isArray(extracted.rows)) extracted.rows = [];

  if (!extracted.rows.length && docAiRecordsForMerge.length) {
    for (const record of docAiRecordsForMerge) {
      if (!record || typeof record !== 'object') continue;
      const values = record.values || {};
      if (!values || typeof values !== 'object' || !Object.keys(values).length) continue;
      extracted.rows.push({ ...values });
      usedDocAiRecords.add(record);
    }
  }

  if (!extracted.rows.length && vertexSpecValues) {
    extracted.rows.push({ ...vertexSpecValues });
  }

  if (Array.isArray(extracted.rows) && extracted.rows.length) {
    for (const row of extracted.rows) {
      if (!row || typeof row !== 'object') continue;
      const docMatch = bestRowMatchToSpec(row, docAiRecordsForMerge, usedDocAiRecords);
      if (docMatch && docMatch.values) {
        const patch = safeMergeSpec(row, docMatch.values);
        if (patch && Object.keys(patch).length) {
          Object.assign(row, patch);
        }
        usedDocAiRecords.add(docMatch);
      }
      if (vertexSpecValues) {
        const patch = safeMergeSpec(row, vertexSpecValues);
        if (patch && Object.keys(patch).length) {
          Object.assign(row, patch);
        }
      }
    }
  }
  
  if (docAiRecordsForMerge.length && Array.isArray(extracted.rows)) {
    for (const record of docAiRecordsForMerge) {
      if (!record || typeof record !== 'object') continue;
      if (usedDocAiRecords.has(record)) continue;
      const patch = safeMergeSpec({}, record.values || {});
      if (patch && Object.keys(patch).length) {
        extracted.rows.push(patch);
      }
    }
  }

  ensureExtractedShape(extracted);
  const mergeSkuCandidates = (...sources) => {
    const skuMap = new Map();
    const pushSku = (value) => {
      if (value == null) return;
      const raw = typeof value === 'string' ? value : String(value);
      const trimmed = raw.trim();
      if (!trimmed) return;
      if (trimmed === '[object Object]') return;
      const norm = normalizeCode(trimmed);
      if (!norm) return;
      if (!skuMap.has(norm)) skuMap.set(norm, trimmed);
    };
    for (const list of sources) {
      if (!Array.isArray(list)) continue;
      for (const item of list) pushSku(item);
    }
    return Array.from(skuMap.values()).slice(0, 200);
  };
  const docTextForSku = typeof extracted?.text === 'string' && extracted.text
    ? extracted.text
    : (previewText || '');
  const skuFromTables = pickSkuListFromTables(extracted);
  const skuFromSystem = expandFromCodeSystem(extracted, blueprint, docTextForSku);
  const skuFromText = harvestMpnCandidates(docTextForSku, extracted?.series);
  const baseCodes = Array.isArray(extracted?.codes) ? extracted.codes : [];
  const mergedSkuList = mergeSkuCandidates(baseCodes, skuFromTables, skuFromSystem, skuFromText);
  extracted.codes = mergedSkuList;
  extracted.mpn_list = mergedSkuList;
  const rawJsonPayload = {};
  if (docAiResult && (docAiText || docAiTables.length)) rawJsonPayload.docai = docAiResult;
  if (vertexClassification) rawJsonPayload.vertex_classify = vertexClassification;
  if (vertexExtractValues && Object.keys(vertexExtractValues).length) {
    rawJsonPayload.vertex_extract = vertexExtractValues;
  }
  if (typeof extracted?.doc_type === 'string' && extracted.doc_type) {
    rawJsonPayload.doc_type = extracted.doc_type;
  }
  if (Object.keys(rawJsonPayload).length) {
    if (!Array.isArray(extracted.rows) || !extracted.rows.length) {
      extracted.rows = [{}];
    }
    for (const row of extracted.rows) {
      if (!row || typeof row !== 'object') continue;
      if (row.raw_json == null) {
        row.raw_json = rawJsonPayload;
      }
    }
  }

  if (detectedBrand && extracted && typeof extracted === 'object') {
    const brandValue = String(extracted.brand || '').trim().toLowerCase();
    if (!brandValue || brandValue === 'unknown') {
      extracted.brand = detectedBrand;
    }
    if (Array.isArray(extracted.rows)) {
      for (const row of extracted.rows) {
        if (!row || typeof row !== 'object') continue;
        const rowBrand = String(row.brand || '').trim().toLowerCase();
        if (!rowBrand || rowBrand === 'unknown') {
          row.brand = detectedBrand;
        }
      }
    }
  }

  const canonicalRuntimeSpecKeys = new Set();
  const sanitizeSpecRows = (rows) => {
    if (!Array.isArray(rows)) return [];
    return rows.map((row) => {
      if (!row || typeof row !== 'object') return {};
      const out = {};
      for (const [rawKey, rawValue] of Object.entries(row)) {
        const key = String(rawKey || '').trim();
        if (!key) continue;
        const lower = key.toLowerCase();
        if (META_KEYS.has(lower) || BASE_KEYS.has(lower) || SKIP_SPEC_KEYS.has(lower)) {
          const existing = Object.prototype.hasOwnProperty.call(out, lower) ? out[lower] : undefined;
          if (!Object.prototype.hasOwnProperty.call(out, lower) || existing == null || existing === '') {
            out[lower] = rawValue;
          }
          continue;
        }
        if (lower.startsWith('_')) {
          const metaKey = lower;
          const existing = Object.prototype.hasOwnProperty.call(out, metaKey) ? out[metaKey] : undefined;
          if (!Object.prototype.hasOwnProperty.call(out, metaKey) || existing == null || existing === '') {
            out[metaKey] = rawValue;
          }
          continue;
        }
        const canon = normalizeSpecKeyName(key);
        if (!canon) continue;
        const mapped = SPEC_KEY_ALIAS_MAP.get(canon) || canon;
        canonicalRuntimeSpecKeys.add(mapped);
        const existing = Object.prototype.hasOwnProperty.call(out, mapped) ? out[mapped] : undefined;
        if (!Object.prototype.hasOwnProperty.call(out, mapped) || existing == null || existing === '') {
          out[mapped] = rawValue;
        }
      }
      return out;
    });
  };

  if (Array.isArray(extracted?.rows) && extracted.rows.length) {
    extracted.rows = sanitizeSpecRows(extracted.rows);
  }

  const autoAddKeys = Array.from(canonicalRuntimeSpecKeys);
  if (process.env.AUTO_ADD_FIELDS === '1' && family && autoAddKeys.length) {
    try {
      const { rows } = await db.query(
        'SELECT public.ensure_dynamic_spec_columns($1, $2::jsonb) AS created',
        [family, JSON.stringify(autoAddKeys)]
      );
      const created = rows?.[0]?.created;
      if (Array.isArray(created) && created.length) {
        console.log('[schema] added columns', created);
      }
    } catch (err) {
      console.warn('[schema] ensure_dynamic_spec_columns failed:', err?.message || err);
    }
  }

  // 🔹 이 변수가 "데이터시트 분석에서 바로 뽑은 MPN 리스트"가 됨
  let codes = [];
  if (!code) {
    const skuFromTable = pickSkuListFromTables(extracted);
    const docText = extracted?.text || previewText || '';
    codes = skuFromTable.length ? skuFromTable : expandFromCodeSystem(extracted, blueprint, docText);
    const maxEnv = Number(process.env.FIRST_PASS_CODES || FIRST_PASS_CODES || 20);
    const maxCodes = Number.isFinite(maxEnv) && maxEnv > 0 ? maxEnv : 20;
    if (codes.length > maxCodes) codes = codes.slice(0, maxCodes);
  }

  // 🔹 후보(candidates)가 아직 비었고, 방금 수집한 codes가 있으면 candidates로 승격
  if (!candidates.length && codes.length) {
    const merged = [];
    const seen = new Set();
    for (const raw of codes) {
      const trimmed = typeof raw === 'string' ? raw.trim() : String(raw || '');
      if (!trimmed) continue;
      const norm = normalizeCode(trimmed);
      if (seen.has(norm)) continue;
      seen.add(norm);
      merged.push(trimmed);
    }
    if (merged.length) candidates = merged;
  }

  // 🔹 “애초에 분석단계에서 여러 MPN을 리스트업” — 추출 결과에 명시적으로 부착
  if (extracted && typeof extracted === 'object') {
    const list = (Array.isArray(codes) ? codes : []).filter(Boolean);
    const merged = mergeSkuCandidates(extracted.codes, list);
    extracted.codes = merged;        // <- 최종 MPN 배열
    extracted.mpn_list = merged;     // <- 동의어(외부에서 쓰기 쉽도록)
  }

    let orderingSectionRanks = [];
  if (!code && !codes.length) {
    let fullText = '';
    try { fullText = await readText(gcsUri, 300 * 1024) || ''; } catch {}

    const fromTypes = extractPartNumbersFromTypesTables(fullText, FIRST_PASS_CODES * 4); // TYPES 표 우선
    orderingSectionRanks = rankPartNumbersFromOrderingSections(fullText, FIRST_PASS_CODES);
    const fromSeries = extractPartNumbersBySeriesHeuristic(fullText, FIRST_PASS_CODES * 4);
    console.log(`[PATH] pns={tables:${fromTypes.length}, body:${orderingSectionRanks.length}} combos=0`);
    // 가장 신뢰 높은 순서로 병합
    const picks = fromTypes.length ? fromTypes : (orderingSectionRanks.length ? orderingSectionRanks : fromSeries);

    if (!candidates.length && picks.length) {
      const merged = [];
      const seen = new Set();
      for (const p of picks) {
        const raw = typeof p === 'string' ? p : p?.code;
        const trimmed = typeof raw === 'string' ? raw.trim() : '';
        if (!trimmed) continue;
        const norm = normalizeCode(trimmed);
        if (seen.has(norm)) continue;
        seen.add(norm);
        merged.push(trimmed);
      }
      if (merged.length) {
        candidates = merged;
        // 🔹 types/order/series 휴리스틱으로도 찾은 경우, 이것도 추출 결과에 반영
        if (extracted && typeof extracted === 'object') {
          const uniq = mergeSkuCandidates(extracted.codes, merged);
          extracted.codes = uniq;
          extracted.mpn_list = uniq;
        }
      }
    }

    // 분할 여부는 별도 판단. 여기서는 후보만 모아둠.
    // extracted.rows는 건드리지 않음.
  }

    const orderingRankScore = Array.isArray(orderingSectionRanks)
    ? orderingSectionRanks.reduce((sum, item) => sum + Number(item?.score || 0), 0)
    : 0;
  const docTypeResolved = resolveDocTypeFromExtraction(
    extracted,
    extracted?.text || previewText || '',
    {
      vertexDocType: vertexClassification?.doc_type || vertexClassification?.docType || null,
      orderingHits: orderingSectionRanks,
      orderingHitCount: Array.isArray(orderingSectionRanks) ? orderingSectionRanks.length : 0,
      orderingScore: orderingRankScore,
      orderingInfo: extracted?.ordering_info,
    },
  );
  if (docTypeResolved) {
    extracted.doc_type = docTypeResolved;
  }
  if (typeof extracted?.doc_type === 'string' && extracted.doc_type) {
    const normalizedDocType = extracted.doc_type.trim();
    if (normalizedDocType && Array.isArray(extracted.rows)) {
      for (const row of extracted.rows) {
        if (!row || typeof row !== 'object') continue;
        if (row.doc_type == null || row.doc_type === '') {
          row.doc_type = normalizedDocType;
        }
      }
    }
  }

  const extractedText = extracted?.text || previewText || '';
  const brandHintSeed = pickBrandHint(
    overridesBrand,
    effectiveBrand,
    brand,
    extracted?.brand,
    detectedBrand,
    vertexClassification?.brand,
  );

  let brandResolution = null;
  try {
    brandResolution = await resolveBrand({ rawText: extractedText, hint: brandHintSeed });
  } catch (err) {
    console.warn('[brand resolve] failed:', err?.message || err);
  }

  const brandEffectiveResolved = pickBrandHint(
    brandResolution?.brand_effective,
    brandHintSeed,
    effectiveBrand,
    extracted?.brand,
    detectedBrand,
  ) || 'unknown';

  let brandSource = brandResolution?.source || null;
  if (!brandSource || brandSource === 'none') {
    brandSource = brandHintSeed ? 'hint' : 'none';
  }
  if (!brandEffectiveResolved || brandEffectiveResolved.toLowerCase() === 'unknown') {
    brandSource = 'none';
  }

  if (extracted && typeof extracted === 'object') {
    extracted.brand = brandEffectiveResolved;
  }

  let runtimeVariantKeys = Array.isArray(variantKeys) ? [...variantKeys] : [];
  const blueprintVariantKeys = runtimeVariantKeys.slice();


  // 커버 추출 비활성(요청에 따라 완전 OFF)
  let coverUri = null;
  if (/^(1|true|on)$/i.test(process.env.COVER_CAPTURE || '0')) {
    try {
      const bForCover = brandEffectiveResolved || 'unknown';
      const cForCover = code || extracted.rows?.[0]?.code || path.parse(fileName).name;
      coverUri = await withTimeout(
        extractCoverToGcs(gcsUri, { family, brand: bForCover, code: cForCover }),
        Math.min(30000, Math.round(BUDGET * 0.15)),
        'cover',
      );
    } catch (e) { console.warn('[cover fail]', e?.message || e); }
  }

  if (code) {
    const trimmedCode = String(code || '').trim();
    if (trimmedCode) {
      const norm = normalizeCode(trimmedCode);
      if (!candidates.some((c) => normalizeCode(c) === norm)) {
        candidates = [trimmedCode, ...candidates];
      }
    }
  }

  // 레코드 구성
  const records = [];
  const now = new Date();
  const brandName = brandEffectiveResolved || 'unknown';
  const baseSeries = series || code || null;
  const runtimeMeta = {
    brand_source: brandSource ?? null,
    variant_keys_runtime: Array.isArray(runtimeVariantKeys) ? runtimeVariantKeys : [],
  };
  const hasRuntimeMeta =
    runtimeMeta.brand_source != null ||
    (Array.isArray(runtimeMeta.variant_keys_runtime) && runtimeMeta.variant_keys_runtime.length > 0);

  let variantColumnsEnsured = !USE_VARIANT_KEYS;
  if (USE_VARIANT_KEYS) {
    try {
      const {
        detected: inferredKeys = [],
        newKeys: freshKeys = [],
        details: discoveryDetails = [],
      } = await inferVariantKeys({
        family,
        brand: brandName,
        series: baseSeries,
        blueprint,
        extracted,
      });

      if (Array.isArray(inferredKeys) && inferredKeys.length) {
        const brandSlug = normalizeSlug(brandName);
        const seriesSlug = normalizeSlug(baseSeries);
        let syncedVariantKeys = null;
        let syncEnsured = false;
        try {
          await db.query(
            `SELECT public.upsert_variant_keys($1,$2,$3,$4::jsonb)`,
            [family, brandSlug, seriesSlug, JSON.stringify(inferredKeys)],
          );
        } catch (err) {
          console.warn('[variant] upsert_variant_keys failed:', err?.message || err);
        }

        try {
          const { rows: syncRows } = await db.query(
            `SELECT public.sync_variant_keys_from_recipes($1) AS ingest_options`,
            [family],
          );
          const ingestOptions = syncRows?.[0]?.ingest_options ?? syncRows?.[0]?.sync_variant_keys_from_recipes ?? null;
          if (ingestOptions && typeof ingestOptions === 'object') {
            if (!blueprint.ingestOptions || typeof blueprint.ingestOptions !== 'object') {
              blueprint.ingestOptions = {};
            }
            if (Array.isArray(ingestOptions.variant_keys)) {
              syncedVariantKeys = ingestOptions.variant_keys
                .map((key) => normalizeSpecKeyName(key) || (typeof key === 'string' ? key.trim() : null))
                .filter((key) => typeof key === 'string' && key.length > 0);
              blueprint.ingestOptions.variant_keys = [...syncedVariantKeys];
              blueprint.variant_keys = [...syncedVariantKeys];
            }
          }
          syncEnsured = true;
        } catch (err) {
          console.warn('[variant] sync_variant_keys_from_recipes failed:', err?.message || err);
        }

        const mergedVariant = new Set(variantKeys);
        for (const key of inferredKeys) mergedVariant.add(key);
        if (Array.isArray(syncedVariantKeys)) {
          for (const key of syncedVariantKeys) mergedVariant.add(key);
        }
        variantKeys = Array.from(mergedVariant);

        const mergedAllowed = new Set(allowedKeys);
        for (const key of variantKeys) mergedAllowed.add(key);
        allowedKeys = Array.from(mergedAllowed);

        if (!blueprint.ingestOptions || typeof blueprint.ingestOptions !== 'object') {
          blueprint.ingestOptions = {};
        }
        blueprint.ingestOptions.variant_keys = variantKeys;
        blueprint.variant_keys = variantKeys;
        blueprint.allowedKeys = Array.isArray(blueprint.allowedKeys)
          ? Array.from(new Set([...blueprint.allowedKeys, ...variantKeys]))
          : [...allowedKeys];

        if (syncEnsured) {
          variantColumnsEnsured = true;
        }

        if (!disableEnsure && !variantColumnsEnsured) {
          try {
            await ensureBlueprintVariantColumns(family);
            variantColumnsEnsured = true;
          } catch (err) {
            console.warn('[variant] ensure_blueprint_variant_columns failed:', err?.message || err);
          }
        }

        if (Array.isArray(freshKeys) && freshKeys.length) {
          try {
            const { rows: materializedRows } = await db.query(
              `SELECT public.materialize_variant_columns($1) AS updated`,
              [family],
            );
            const updatedCount = Number(
              materializedRows?.[0]?.updated ?? materializedRows?.[0]?.materialize_variant_columns ?? 0,
            );
            if (updatedCount > 0) {
              console.log('[variant] materialized variant columns', {
                family,
                brand: brandName,
                series: baseSeries,
                updated: updatedCount,
                keys: freshKeys,
              });
            }
          } catch (err) {
            console.warn('[variant] materialize_variant_columns failed:', err?.message || err);
          }
        }
      }

      if (Array.isArray(freshKeys) && freshKeys.length) {
        console.log('[variant] detected new keys', { family, brand: brandName, series: baseSeries, keys: freshKeys });
        if (Array.isArray(discoveryDetails) && discoveryDetails.length) {
          const stats = discoveryDetails
            .filter((item) => freshKeys.includes(item?.key))
            .map((item) => {
              const coverage = Number(item?.coverage ?? 0);
              const pnRatio = Number(item?.pnRatio ?? 0);
              return {
                key: item?.key,
                coverage: Number.isFinite(coverage) ? Number(coverage.toFixed(3)) : null,
                cardinality: item?.cardinality ?? null,
                pn_ratio: Number.isFinite(pnRatio) ? Number(pnRatio.toFixed(3)) : null,
              };
            })
            .filter((item) => item.key);
          if (stats.length) {
            console.log('[variant] discovery stats', {
              family,
              brand: brandName,
              series: baseSeries,
              stats,
            });
          }
        }
      }
    } catch (err) {
      console.warn('[variant] inferVariantKeys failed:', err?.message || err);
    }
  }

  if (!disableEnsure) {
    await ensureSpecsTableByFamily(family, qualified);
    if (!variantColumnsEnsured) {
      try {
        await ensureBlueprintVariantColumns(family);
        variantColumnsEnsured = true;
      } catch (err) {
        console.warn('[variant] ensure_blueprint_variant_columns fallback failed:', err?.message || err);
      }
    }
  }

  await ensureSpecColumnsForBlueprint(qualified, blueprint);

  const rawRows = Array.isArray(extracted?.rows) && extracted.rows.length ? extracted.rows : [];

  let orderingDomains = null;
  let orderingOverride = null;
  let orderingTextSources = [];
  if (USE_CODE_RULES) {
    const orderingCollection = collectOrderingDomains({
      orderingInfo: extracted?.ordering_info,
      previewText,
      docAiText,
      docAiTables,
    });
    orderingDomains = orderingCollection?.domains ?? null;
    orderingTextSources = Array.isArray(orderingCollection?.textSources)
      ? orderingCollection.textSources
          .map((txt) => (typeof txt === 'string' ? txt : String(txt ?? '')))
          .map((txt) => txt.trim())
          .filter(Boolean)
      : [];
    if (!orderingDomains) {
      orderingOverride = buildTyOrderingFallback({
        baseSeries,
        orderingInfo: extracted?.ordering_info,
        previewText,
        docAiText,
      });
      if (orderingOverride) orderingDomains = orderingOverride.domains;
    }
    if (!orderingDomains) {
      const orderingWindowText = typeof extracted?.ordering_info?.text === 'string'
        ? extracted.ordering_info.text
        : '';
      const orderingHaystack = [orderingWindowText, previewText, docAiText]
        .filter((chunk) => typeof chunk === 'string' && chunk.trim())
        .join('\n');
      if (orderingWindowText.trim() || (orderingHaystack && ORDERING_SECTION_RE.test(orderingHaystack))) {
        try {
          const recipeInput = orderingWindowText.trim().length >= 40
            ? orderingWindowText
            : (gcsUri || orderingWindowText);
          const recipe = await extractOrderingRecipe(recipeInput);
          const variantDomains = recipe?.variant_domains;
          const normalizedDomains = {};
          if (variantDomains && typeof variantDomains === 'object') {
            for (const [rawKey, rawValue] of Object.entries(variantDomains)) {
              const key = String(rawKey || '').trim();
              if (!key) continue;
              const values = Array.isArray(rawValue) ? rawValue : [rawValue];
              const seen = new Set();
              const list = [];
              for (const candidate of values) {
                if (candidate == null) continue;
                const str = String(candidate).trim();
                const marker = str === '' ? '__EMPTY__' : str.toLowerCase();
                if (seen.has(marker)) continue;
                seen.add(marker);
                list.push(str);
              }
              if (list.length) normalizedDomains[key] = list;
            }
          }
          if (Object.keys(normalizedDomains).length) {
            orderingDomains = normalizedDomains;
            if (!pnTemplate && typeof recipe?.pn_template === 'string' && recipe.pn_template.trim()) {
              pnTemplate = recipe.pn_template.trim();
            }
          }
        } catch (err) {
          console.warn('[ordering] recipe extract failed:', err?.message || err);
        }
      }
    }
  }

  const orderingDomainKeys = Object.keys(orderingDomains || {});
  if (USE_VARIANT_KEYS) {
    let aiVariantKeys = [];
    const rawOrderingText = orderingTextSources.length ? orderingTextSources.join('\n') : '';
    const detectionInput = rawOrderingText || extractedText || '';
    if (detectionInput.trim()) {
      try {
        aiVariantKeys = await detectVariantKeys({
          rawText: detectionInput,
          family,
          blueprintVariantKeys: blueprint?.variant_keys,
          allowedKeys: blueprint?.allowedKeys,
        });
      } catch (err) {
        console.warn('[variant] runtime detect failed:', err?.message || err);
      }
    }
    runtimeVariantKeys = mergeVariantKeyLists(
      VARIANT_MAX_CARDINALITY,
      Array.isArray(aiVariantKeys) ? aiVariantKeys : [],
      orderingDomainKeys,
      blueprintVariantKeys,
    );
  } else {
    runtimeVariantKeys = [];
  }

  console.log('[PATH] brand resolved', {
    runId,
    family,
    hint: brandHintSeed || null,
    effective: brandEffectiveResolved,
    source: brandSource,
    vkeys_runtime: runtimeVariantKeys,
  });

  if (orderingDomains) {
    const orderingKeys = Object.keys(orderingDomains)
      .map((key) => String(key || '').trim())
      .filter(Boolean);
    if (orderingKeys.length) {
      const orderingTemplate = extractOrderingTemplate(extracted?.ordering_info);
      const baseSeriesForOrdering = (
        orderingOverride?.series
          || extracted?.rows?.[0]?.series_code
          || extracted?.rows?.[0]?.series
          || baseSeries
          || series
          || code
          || null
      );
      if (!pnTemplate && orderingTemplate) pnTemplate = orderingTemplate;
      // 템플릿이 여전히 없으면, 여기서 즉시 학습해 폭발 단계에 반영
      if (USE_PN_TEMPLATE && !pnTemplate) {
        try {
          const fullText = await readText(gcsUri, 300 * 1024).catch(() => '');
          if (fullText) {
            const { learnPnTemplate, upsertExtractionRecipe } = require('./pn-grammar');
            const tpl = await learnPnTemplate({
              family,
              brand: brandName,
              series: baseSeriesForOrdering,
              docText: fullText,
              rows: Array.isArray(extracted?.rows) && extracted.rows.length ? extracted.rows : rawRows,
            });
            if (tpl) {
              pnTemplate = tpl;
              await upsertExtractionRecipe({
                family,
                brand: brandName,
                series: baseSeriesForOrdering,
                pnTemplate: tpl,
              });
            }
          }
        } catch (_) {}
      }
      let templateForOrdering = orderingTemplate
        || orderingOverride?.pnTemplate
        || pnTemplate
        || blueprint?.ingestOptions?.pn_template
        || blueprint?.ingestOptions?.pnTemplate
        || null;
      const orderingBase = {
        brand: brandName,
        series: baseSeriesForOrdering,
        series_code: baseSeriesForOrdering,
        values: orderingDomains,
      };
      let orderingHaystack = '';
      if (Array.isArray(orderingTextSources) && orderingTextSources.length) {
        orderingHaystack = orderingTextSources
          .map((txt) => (typeof txt === 'string' ? txt : String(txt ?? '')))
          .filter((txt) => txt && txt.trim())
          .join('\n');
      }

      const sampleStats = { attempted: 0, accepted: 0 };
      const sampleLimitRaw = Number(process.env.ORDERING_TEMPLATE_SAMPLE_LIMIT ?? 25);
      const sampleLimit = Number.isFinite(sampleLimitRaw) && sampleLimitRaw > 0
        ? Math.min(Math.floor(sampleLimitRaw), 200)
        : 25;
      const minHitRateRaw = Number(process.env.ORDERING_TEMPLATE_MIN_HITRATE ?? 0.6);
      const minHitRate = Number.isFinite(minHitRateRaw)
        ? Math.min(Math.max(minHitRateRaw, 0), 1)
        : 0.6;

      if (templateForOrdering) {
        try {
          explodeToRows(orderingBase, {
            variantKeys: orderingKeys,
            pnTemplate: templateForOrdering,
            haystack: orderingHaystack,
            textContainsExact,
            previewOnly: true,
            maxTemplateAttempts: sampleLimit,
            onTemplateRender: ({ accepted }) => {
              if (sampleStats.attempted >= sampleLimit) return;
              sampleStats.attempted += 1;
              if (accepted) sampleStats.accepted += 1;
            },
          });
        } catch (err) {
          console.warn('[ordering] template preview failed:', err?.message || err);
        }
        const attempts = sampleStats.attempted;
        const hits = sampleStats.accepted;
        const hitRate = attempts > 0 ? hits / attempts : 0;
        if (!attempts || hitRate < minHitRate) {
          console.warn('[ordering] template rejected due to low hit rate', {
            attempts,
            hits,
            hitRate,
            minHitRate,
          });
          templateForOrdering = null;
        }
      }

      const explodedOrdering = explodeToRows(orderingBase, {
        variantKeys: orderingKeys,
        pnTemplate: templateForOrdering,
        haystack: orderingHaystack,
        textContainsExact,
      });
      if (Array.isArray(explodedOrdering) && explodedOrdering.length) {
        const existingCodes = new Set();
        for (const row of rawRows) {
          if (!row || typeof row !== 'object') continue;
          const seed = row.code ?? row.pn ?? row.part_number;
          if (!seed) continue;
          const norm = String(seed).trim().toLowerCase();
          if (norm) existingCodes.add(norm);
        }
        const appendedRows = [];
        for (const generated of explodedOrdering) {
          if (!generated || typeof generated !== 'object') continue;
          const codeValue = typeof generated.code === 'string' ? generated.code.trim() : '';
          if (!codeValue) continue;
          const codeNorm = codeValue.toLowerCase();
          if (existingCodes.has(codeNorm)) continue;
          existingCodes.add(codeNorm);
          const values = generated.values && typeof generated.values === 'object' ? generated.values : {};
          const normalizedSeries =
            values.series_code
              ?? values.series
              ?? orderingBase.series_code
              ?? orderingBase.series
              ?? null;
          const newRow = {
            ...values,
            code: codeValue,
            code_norm: generated.code_norm || codeNorm,
            verified_in_doc: true,
          };
          if (normalizedSeries && newRow.series == null) newRow.series = normalizedSeries;
          if (normalizedSeries && newRow.series_code == null) newRow.series_code = normalizedSeries;
          appendedRows.push(newRow);
          rawRows.push(newRow);
        }
        if (appendedRows.length) {
          if (!Array.isArray(extracted.rows) || !extracted.rows.length) {
            extracted.rows = rawRows;
          }
          const lowerOrderingKeys = orderingKeys
            .map((k) => String(k).trim().toLowerCase())
            .filter(Boolean);
          const allowedLower = new Set(
            (allowedKeys || []).map((k) => String(k || '').trim().toLowerCase()).filter(Boolean)
          );
          for (const key of lowerOrderingKeys) {
            if (!variantKeys.includes(key)) variantKeys.push(key);
            if (!allowedLower.has(key)) {
              allowedKeys.push(key);
              allowedLower.add(key);
            }
            if (Array.isArray(runtimeVariantKeys) && !runtimeVariantKeys.includes(key)) {
              runtimeVariantKeys.push(key);
            }
          }
        }
      }
    }
  }

  const runtimeSpecKeys = gatherRuntimeSpecKeys(rawRows);

    if (family && extracted && AUTO_ALIAS_LEARN && AUTO_ALIAS_LIMIT) {
    try {
      const { added } = await learnAndPersistAliases({
        family,
        brand: brandName,
        series: baseSeries,
        blueprint,
        extracted,
      });
      if (added > 0) {
        console.log('[auto-alias] learned aliases', { family, brand: brandName, series: baseSeries, added });
      }
    } catch (err) {
      console.warn('[auto-alias] failed:', err?.message || err);
    }
  }

  if (AUTO_ADD_FIELDS && AUTO_ADD_FIELDS_LIMIT && runtimeSpecKeys.size) {
    try {
      const knownColumns = await getColumnsOf(qualified);
      const pending = [];
      const seen = new Set();
      for (const rawKey of runtimeSpecKeys) {
        const trimmed = String(rawKey || '').trim();
        if (!trimmed) continue;
        const lower = trimmed.toLowerCase();
        if (seen.has(lower)) continue;
        seen.add(lower);
        if (knownColumns.has(lower)) continue;
        if (RESERVED_SPEC_KEYS.has(lower)) continue;
        pending.push(trimmed);
        if (pending.length >= AUTO_ADD_FIELDS_LIMIT) break;
      }

      if (pending.length) {
        const remaining = new Set(pending);
        const sample = {};
        for (const row of rawRows) {
          if (!row || typeof row !== 'object') continue;
          for (const key of pending) {
            if (!remaining.has(key)) continue;
            if (Object.prototype.hasOwnProperty.call(row, key)) {
              sample[key] = row[key];
              remaining.delete(key);
            }
          }
          if (!remaining.size) break;
        }

        await ensureSpecColumnsForKeys(qualified, pending, sample);
      }
    } catch (err) {
      console.warn('[schema] ensureSpecColumnsForKeys failed:', err?.message || err);
    }
  }

  colTypes = await getColumnTypes(qualified);

  if (process.env.AUTO_FIX_BLUEPRINT_TYPES === '1' && colTypes instanceof Map && colTypes.size && family) {
    const currentFields = blueprint?.fields && typeof blueprint.fields === 'object' ? blueprint.fields : {};
    const patch = {};
    for (const [col, t] of colTypes.entries()) {
      if (t === 'numeric' || t === 'int' || t === 'bool') {
        const now = currentFields[col];
        if (!now || String(now).toLowerCase() === 'text') {
          patch[col] = t === 'int' ? 'int' : t;
        }
      }
    }
    if (Object.keys(patch).length) {
      await db.query(
        `UPDATE public.component_spec_blueprint
           SET fields_json = fields_json || $2::jsonb,
               version = COALESCE(version,0)+1,
               updated_at = now()
         WHERE family_slug = $1`,
        [family, JSON.stringify(patch)]
      );
      if (blueprint && typeof blueprint === 'object') {
        if (!blueprint.fields || typeof blueprint.fields !== 'object') {
          blueprint.fields = {};
        }
        Object.assign(blueprint.fields, patch);
      }
    }
  }

  const aiCanonicalMap = new Map();
  const aiCanonicalMapLower = new Map();
  if (process.env.AUTO_CANON_KEYS === '1' && runtimeSpecKeys.size) {
    const specCols = colTypes ? Array.from(colTypes.keys()) : [];
    const blueprintFieldKeys = blueprint?.fields && typeof blueprint.fields === 'object'
      ? Object.keys(blueprint.fields).map((k) => String(k || '').trim().toLowerCase()).filter(Boolean)
      : [];
    const knownKeys = Array.from(new Set([
      ...specCols,
      ...blueprintFieldKeys,
      ...(Array.isArray(variantKeys) ? variantKeys : []),
    ]));

    try {
      const { map, newKeys } = await aiCanonicalizeKeys(
        family,
        Array.from(runtimeSpecKeys),
        knownKeys
      );

      const knownLower = new Set(knownKeys.map((k) => String(k || '').trim().toLowerCase()).filter(Boolean));
      const newKeySet = new Set((Array.isArray(newKeys) ? newKeys : []).map((k) => String(k || '').trim()).filter(Boolean));
      const newCanonKeys = [];
      for (const [orig, info] of Object.entries(map || {})) {
        const trimmedOrig = String(orig || '').trim();
        if (!trimmedOrig) continue;
        const baseLower = trimmedOrig.toLowerCase();
        let canonical = String(info?.canonical || '').trim();
        let action = info?.action === 'map' ? 'map' : 'new';
        let conf = Number(info?.conf || 0);
        if (!Number.isFinite(conf)) conf = 0;

        let finalKey = null;
        if (action === 'map' && canonical) {
          const lowerCanon = canonical.toLowerCase();
          if (knownLower.has(lowerCanon)) {
            finalKey = lowerCanon;
          } else {
            finalKey = normalizeSpecKeyName(canonical) || lowerCanon || null;
          }
          if (!finalKey) {
            finalKey = baseLower;
            action = 'new';
          }
        } else {
          const normalized = normalizeSpecKeyName(canonical || trimmedOrig);
          finalKey = normalized || baseLower;
          action = 'new';
        }

        if (!finalKey || META_KEYS.has(finalKey) || BASE_KEYS.has(finalKey)) continue;
        const payload = { canonical: finalKey, action, conf };
        aiCanonicalMap.set(trimmedOrig, payload);
        aiCanonicalMapLower.set(baseLower, payload);
        if (action === 'new' || newKeySet.has(trimmedOrig)) newCanonKeys.push(finalKey);
      }

      if (process.env.AUTO_ADD_FIELDS === '1' && newCanonKeys.length) {
        const uniqueNew = Array.from(new Set(newCanonKeys.filter(Boolean)));
        const limitRaw = Number(process.env.AUTO_ADD_FIELDS_LIMIT || '50');
        const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? limitRaw : uniqueNew.length;
        const target = uniqueNew.slice(0, limit);
        if (target.length) {
          try {
            const { rows } = await db.query(
              'SELECT public.ensure_dynamic_spec_columns($1,$2::jsonb) AS created',
              [family, JSON.stringify(target)]
            );
            console.log('[schema] added columns', rows?.[0]?.created);
          } catch (err) {
            console.warn('[schema] ensure_dynamic_spec_columns failed:', err?.message || err);
          }

          for (const key of target) {
            if (!key) continue;
            if (colTypes && !colTypes.has(key)) colTypes.set(key, 'text');
            if (!allowedKeys.includes(key)) allowedKeys.push(key);
          }

          allowedKeys = Array.from(new Set(allowedKeys));

          if (Array.isArray(blueprint?.allowedKeys)) {
            blueprint.allowedKeys = Array.from(new Set([...blueprint.allowedKeys, ...target]));
          } else {
            blueprint.allowedKeys = [...target];
          }
        }
      }

      const minCanonConf = Number(process.env.AUTO_CANON_MIN_CONF || '0.66');
      for (const [orig, info] of aiCanonicalMap.entries()) {
        if (info.action !== 'map' || !info.canonical || info.conf < minCanonConf) continue;
        try {
          await db.query('SELECT public.upsert_spec_alias($1,$2,$3)', [family, orig, info.canonical]);
        } catch (_) {
          /* ignore alias cache errors */
        }
      }
    } catch (err) {
      console.warn('[canon] aiCanonicalizeKeys failed:', err?.message || err);
      aiCanonicalMap.clear();
      aiCanonicalMapLower.clear();
    }
  }

  const mpnsFromDoc = harvestMpnCandidates(
    extracted?.text ?? '',
    (baseSeries || series || code || '')
  );
  const mpnNormFromDoc = new Set(mpnsFromDoc.map((m) => normalizeCode(m)).filter(Boolean));

  const candidateMap = [];
  const candidateNormSet = new Set();
  for (const cand of candidates) {
    const trimmed = typeof cand === 'string' ? cand.trim() : String(cand || '');
    if (!trimmed) continue;
    const norm = normalizeCode(trimmed);
    if (!norm || candidateNormSet.has(norm)) continue;
    candidateNormSet.add(norm);
    candidateMap.push({ raw: trimmed, norm });
  }

  const baseRows = (rawRows.length ? rawRows : [{}]).map((row) => {
    const obj = row && typeof row === 'object' ? { ...row } : {};
    if (obj.brand == null) obj.brand = brandName;
    const fallbackSeries = obj.series_code || obj.series || baseSeries || null;
    if (fallbackSeries != null) {
      if (obj.series == null) obj.series = fallbackSeries;
      if (obj.series_code == null) obj.series_code = fallbackSeries;
    }
    if (obj.datasheet_uri == null) obj.datasheet_uri = gcsUri;
    if (coverUri && obj.cover == null) obj.cover = coverUri;
    return obj;
  });

  let explodedRows = baseRows;
  if (USE_CODE_RULES) {
    const expanded = expandRowsWithVariants(baseRows, {
      variantKeys,
      pnTemplate,
      defaultBrand: brandName,
      defaultSeries: baseSeries,
    });
    if (Array.isArray(expanded) && expanded.length) {
      explodedRows = expanded;
    }
  }
  const physicalCols = new Set(colTypes ? [...colTypes.keys()] : []);
  const allowedSet = new Set((allowedKeys || []).map((k) => String(k || '').trim().toLowerCase()).filter(Boolean));
  const variantSet = new Set(variantKeys);

  const seenCodes = new Set();
  for (const row of explodedRows) {
    const seeds = [];
    const seenSeed = new Set();
    const pushSeed = (val) => {
      if (val == null) return;
      if (Array.isArray(val)) { val.forEach(pushSeed); return; }
      const str = String(val).trim();
      if (!str) return;
      const parts = splitAndCarryPrefix(str);
      if (parts.length > 1) { parts.forEach(pushSeed); return; }
      const normed = str.toLowerCase();
      if (seenSeed.has(normed)) return;
      seenSeed.add(normed);
      seeds.push(str);
    };
    pushSeed(row.code);
    pushSeed(row.mpn);
    pushSeed(row.part_number);
    pushSeed(row.part_no);

    let mpn = seeds.length ? seeds[0] : '';
    if (!mpn && candidateMap.length) mpn = candidateMap[0].raw;
    mpn = String(mpn || '').trim();
    if (!mpn) continue;
    const mpnNorm = normalizeCode(mpn);
    const brandKey = normLower(row.brand || brandName);
    const rec = {};
    const naturalKey = `${brandKey}::${mpnNorm}`;
    if (!mpnNorm || seenCodes.has(naturalKey)) continue;
    seenCodes.add(naturalKey);

    rec.family_slug = family;
    rec.brand = row.brand || brandName;
    rec.pn = mpn;
    if (row.code != null) rec.code = row.code;
    if (!rec.code) rec.code = mpn;
    rec.series_code = row.series_code ?? row.series ?? baseSeries ?? null;
    if (row.series != null && physicalCols.has('series')) rec.series = row.series;
    rec.datasheet_uri = row.datasheet_uri || gcsUri;
    rec.datasheet_url = pickDatasheetUrl(row.datasheet_url, rec.datasheet_uri);
    if (row.mfr_full != null) rec.mfr_full = row.mfr_full;
    let verified;
    if (row.verified_in_doc != null) {
      if (typeof row.verified_in_doc === 'string') {
        verified = row.verified_in_doc.trim().toLowerCase() === 'true';
      } else {
        verified = Boolean(row.verified_in_doc);
      }
    } else {
      verified = candidateNormSet.has(mpnNorm) || mpnNormFromDoc.has(mpnNorm);
    }
    rec.verified_in_doc = Boolean(verified);
    rec.image_uri = row.image_uri || coverUri || null;
    if (coverUri && rec.cover == null) rec.cover = coverUri;
    const displayName = row.display_name || row.displayname || `${rec.brand} ${mpn}`;
    rec.display_name = displayName;
    if (rec.displayname == null && displayName != null) rec.displayname = displayName;
    rec.updated_at = now;
    // persist에서 브랜드 정규화할 때 쓰도록 원문 텍스트 전달
    rec._doc_text = extractedText;
    if (hasRuntimeMeta) {
      rec.raw_json = mergeRuntimeMetadata(row.raw_json, runtimeMeta);
    } else if (row.raw_json != null) {
      rec.raw_json = row.raw_json;
    }

    for (const [rawKey, rawValue] of Object.entries(row)) {
      const key = String(rawKey || '').trim();
      if (!key) continue;
      const lower = key.toLowerCase();
      if (META_KEYS.has(lower) || BASE_KEYS.has(lower)) continue;
      const mapped = aiCanonicalMap.get(key) || aiCanonicalMapLower.get(lower);
      const target = mapped?.canonical || lower;
      if (!target || META_KEYS.has(target) || BASE_KEYS.has(target)) continue;
      if (physicalCols.has(target) || allowedSet.has(target) || variantSet.has(target)) {
        rec[target] = rawValue;
      }
    }

    if (USE_CODE_RULES && blueprint?.code_rules) {
      applyCodeRules(rec.code, rec, blueprint.code_rules, colTypes);
    }
    records.push(rec);
  }

  if (candidateMap.length) {
    const fallbackSeries = baseSeries || null;
    for (const cand of candidateMap) {
      const norm = cand.norm;
      const naturalKey = `${normLower(brandName)}::${norm}`;
      if (seenCodes.has(naturalKey)) continue;
      seenCodes.add(naturalKey);
      const verified = mpnNormFromDoc.has(norm);
      const rec = {
        family_slug: family,
        brand: brandName,
        pn: cand.raw,
        code: cand.raw,
        series_code: fallbackSeries,
        datasheet_uri: gcsUri,
        image_uri: coverUri || null,
        display_name: `${brandName} ${cand.raw}`,
        verified_in_doc: verified,
        updated_at: now,
      };
      if (coverUri) rec.cover = coverUri;
      if (hasRuntimeMeta) {
        rec.raw_json = mergeRuntimeMetadata(rec.raw_json, runtimeMeta);
      }
      if (physicalCols.has('series') && fallbackSeries != null) rec.series = fallbackSeries;
      rec.datasheet_url = pickDatasheetUrl(null, rec.datasheet_uri);
      if (rec.display_name != null && rec.displayname == null) rec.displayname = rec.display_name;
      records.push(rec);
    }
  }

  console.log('[MPNDBG]', {
    picks: candidateMap.length,
    vkeys: Array.isArray(blueprint?.ingestOptions?.variant_keys) ? blueprint.ingestOptions.variant_keys : [],
    vkeys_runtime: runtimeVariantKeys,
    brand_source: brandSource,
    expanded: explodedRows.length,
    recs: records.length,
    colsSanitized: colTypes?.size || 0,
  });

  // 추출/가공 끝난 직후 시점에…
  // 2-1) PN 템플릿이 없으면 문서에서 자동 유도 → recipe에 저장하고 이번 런에도 즉시 사용
  try {
    if (USE_PN_TEMPLATE && !pnTemplate) {
      const fullText = await readText(gcsUri, 300 * 1024);
      const { learnPnTemplate, upsertExtractionRecipe } = require('./pn-grammar');
      const tpl = await learnPnTemplate({
        family,
        brand: brand || extracted.brand,
        series,
        docText: fullText,
        rows: Array.isArray(records) && records.length ? records : extracted.rows,
      });
      if (tpl) {
        await upsertExtractionRecipe({ family, brand: brand || extracted.brand, series, pnTemplate: tpl });
        pnTemplate = tpl; // 이번 런에 바로 적용
      }
    }
  } catch (e) { console.warn('[pn-learn] skipped:', e?.message || e); }

  const processedPayload = {
    started,
    gcsUri,
    family,
    table,
    qualified,
    pnTemplate,
    requiredFields,
    coverUri,
    records,
    rows: records,
    mpnList: Array.isArray(extracted?.mpn_list) ? extracted.mpn_list : [],
    extractedBrand: extracted?.brand || null,
    brandName,
    baseSeries,
    runId,
    run_id: runId,
    jobId,
    job_id: jobId,
    text: extractedText,
    brand: brandEffectiveResolved || extracted?.brand || null,
    brand_detected: detectedBrand || null,
    brand_effective: brandEffectiveResolved || null,
    brand_source: brandSource || null,
    variant_keys_runtime: runtimeVariantKeys,
    ordering_info: extracted?.ordering_info ?? null,
    doc_type: typeof extracted?.doc_type === 'string' ? extracted.doc_type : null,
  };

  console.log(
    '[DIAG] processedPayload recs=%d mpnList=%d docType=%s ordering=%s',
    Array.isArray(records) ? records.length : -1,
    Array.isArray(processedPayload.mpnList) ? processedPayload.mpnList.length : -1,
    processedPayload.doc_type || null,
    processedPayload.ordering_info ? 'yes' : 'no',
  );

  if (Array.isArray(extracted?.codes)) processedPayload.candidateCodes = extracted.codes;
  if (display_name != null) processedPayload.display_name = display_name;
  if (code != null) processedPayload.code = code;
  if (series != null) processedPayload.series = series;

  if (input && typeof input === 'object' && input.skipPersist) {
    return { ok: true, phase: 'process', processed: processedPayload };
  }

  const persistBrand = pickBrandHint(brandEffectiveResolved, overridesBrand, effectiveBrand, detectedBrand, brand);
  const persistOverrides = {
    brand: persistBrand || null,
    code,
    series: overridesSeries ?? series,
    display_name,
    runId,
    run_id: runId,
    jobId,
    job_id: jobId,
  };
  return withDeadline(
    persistProcessedData(processedPayload, persistOverrides),
    HARD_CAP_MS,
    'PERSIST',
  );
  })();

  try {
    return await runnerPromise;
  } finally {
    await releaseLock();
  }
}

async function runAutoIngest(payload = {}) {
  const normalizedPayload = payload && typeof payload === 'object' ? { ...payload } : {};
  const runId = normalizedPayload.runId ?? normalizedPayload.run_id ?? crypto.randomUUID();
  normalizedPayload.runId = runId;
  normalizedPayload.run_id = runId;

  await db.query(
    `
      INSERT INTO public.ingest_run_logs (id, gcs_uri, status, started_at, ts)
      VALUES ($1, $2, 'RUNNING', now(), now())
      ON CONFLICT (id) DO NOTHING
    `,
    [runId, normalizedPayload.gcsUri || normalizedPayload.gsUri || null],
  );

  const watchdogMs = Number(process.env.INGEST_WATCHDOG_MS || 870000);
  const watchdog = setTimeout(async () => {
    try {
      await db.query(
        `
          UPDATE public.ingest_run_logs
             SET status='FAILED', event='WATCHDOG_TIMEOUT', error_message='watchdog timeout', finished_at=now(), ts=now()
           WHERE id = $1 AND status='RUNNING'
        `,
        [runId],
      );
    } catch (err) {
      console.warn('[ingest] watchdog update failed:', err?.message || err);
    }
  }, watchdogMs);
  if (typeof watchdog?.unref === 'function') watchdog.unref();

  try {
    const result = await doIngestPipeline(normalizedPayload, runId);
    const affected = Number(result?.affected ?? result?.rows ?? 0);
    const ok = Boolean(result?.ok) && affected > 0;
    await db.query(
      `
        UPDATE public.ingest_run_logs
           SET status       = $2,
               event        = $3,
               final_table  = $4,
               final_family = $5,
               final_brand  = $6,
               final_code   = $7,
               final_datasheet = $8,
               error_message   = $9,
               finished_at  = now(), ts = now()
         WHERE id = $1
      `,
      [
        runId,
        ok ? 'SUCCEEDED' : 'FAILED',
        ok ? 'PERSIST_DONE' : 'PERSIST_ZERO',
        result?.specs_table || result?.final_table || null,
        result?.family || null,
        result?.brand || null,
        Array.isArray(result?.codes) ? result.codes[0] : (result?.code || null),
        result?.datasheet_uri || null,
        ok
          ? null
          : Array.isArray(result?.reject_reasons)
              ? result.reject_reasons.join(',')
              : null,
      ],
    );
    return result;
  } catch (e) {
    const msg = (e && e.message ? String(e.message) : 'error').slice(0, 500);
    try {
      await db.query(
        `
          UPDATE public.ingest_run_logs
             SET status='FAILED',
                 event='EXCEPTION',
                 error_message=$2,
                 finished_at=now(), ts=now()
           WHERE id=$1
        `,
        [runId, msg],
      );
    } catch (err) {
      console.warn('[ingest] failure update failed:', err?.message || err);
    }
    throw e;
  } finally {
    clearTimeout(watchdog);
    try { await db.query('SELECT pg_advisory_unlock(hashtextextended($1))', [runId]); } catch {}
    try { await db.query('SELECT pg_advisory_unlock(hashtext($1))', [runId]); } catch {}
  }
}

async function persistProcessedData(processed = {}, overrides = {}) {
  const {
    started = Date.now(),
    gcsUri = null,
    family: processedFamily = null,
    family_slug: processedFamilySlug = null,
    table: processedTable = null,
    specs_table: processedSpecsTable = null,
    qualified: qualifiedInput = null,
    pnTemplate = null,
    requiredFields = [],
    coverUri = null,
    records: initialRecords = [],
    rows: processedRowsInput = [],
    mpnList = [],
    extractedBrand = null,
    brandName = null,
    baseSeries = null,
    text: processedText = null,
    brand: processedBrand = null,
    brand_detected: processedDetected = null,
    brand_effective: processedEffective = null,
    brand_source: processedBrandSource = null,
    variant_keys_runtime: processedVariantKeys = [],
    meta: processedMeta = null,
  } = processed || {};

  // persist: Cloud Tasks가 별도 요청으로 호출 → 매번 family/table/colTypes 재확정 필수
  const normalizeFamily = (value) => {
    if (!value) return null;
    const trimmed = String(value).trim();
    return trimmed || null;
  };

  const pickFamily = (...values) => {
    for (const value of values) {
      const normalized = normalizeFamily(value);
      if (normalized) return normalized;
    }
    return null;
  };

  const family = pickFamily(
    processedFamily,
    processedFamilySlug,
    processedMeta?.family,
    processedMeta?.family_slug,
    overrides?.family,
    overrides?.family_slug,
  );

  const sanitizePart = (value) => String(value || '').replace(/[^a-zA-Z0-9_]/g, '');
  const sanitizeIdentifier = (value) => {
    const trimmed = String(value || '').trim();
    if (!trimmed) return '';
    if (trimmed.includes('.')) {
      const [schemaRaw, tableRaw] = trimmed.split('.', 2);
      const schemaSafe = sanitizePart(schemaRaw);
      const tableSafe = sanitizePart(tableRaw);
      if (!schemaSafe || !tableSafe) return '';
      return `${schemaSafe}.${tableSafe}`;
    }
    return sanitizePart(trimmed);
  };

  const extractQualified = (value) => {
    const normalized = sanitizeIdentifier(value);
    if (!normalized) return { table: '', qualified: '' };
    if (normalized.includes('.')) {
      const [schema, tbl] = normalized.split('.', 2);
      return { table: tbl, qualified: `${schema}.${tbl}` };
    }
    return { table: normalized, qualified: '' };
  };

  const tableCandidates = [
    processedSpecsTable,
    processedTable,
    qualifiedInput,
    overrides?.specs_table,
    overrides?.table,
    overrides?.qualified,
  ];

  let table = '';
  let qualified = '';
  for (const candidate of tableCandidates) {
    const { table: tbl, qualified: qual } = extractQualified(candidate);
    if (tbl) {
      table = tbl;
      if (qual) qualified = qual;
      break;
    }
  }

  if (!table && family) {
    try {
      const r = await db.query(
        `SELECT specs_table FROM public.component_registry WHERE family_slug=$1 LIMIT 1`,
        [family],
      );
      const { table: tbl, qualified: qual } = extractQualified(r.rows?.[0]?.specs_table);
      if (tbl) {
        table = tbl;
        if (qual) qualified = qual;
      }
    } catch (err) {
      console.warn('[persist] specs_table lookup failed:', err?.message || err);
    }
  }

  if (!table && family) {
    const fallback = sanitizePart(`${family}_specs`);
    if (fallback) table = fallback;
  }

  if (!table) {
    throw new Error('persist_no_table');
  }

  if (!qualified) {
    qualified = table.includes('.') ? table : `public.${table}`;
  }

  let colTypes = new Map();
  try {
    colTypes = await getColumnTypes(qualified);
  } catch (err) {
    console.warn('[persist] column type fetch failed:', err?.message || err);
    colTypes = new Map();
  }

  const recordsSource = Array.isArray(initialRecords) && initialRecords.length
    ? initialRecords
    : (Array.isArray(processedRowsInput) ? processedRowsInput : []);
  let records = Array.isArray(recordsSource) ? recordsSource : [];
  sanitizeRecordTemplates(records);
  if (Array.isArray(processedRowsInput) && processedRowsInput !== records) {
    sanitizeRecordTemplates(processedRowsInput);
  }
  const runtimeMeta = {
    brand_source: processedBrandSource ?? null,
    variant_keys_runtime: Array.isArray(processedVariantKeys) ? processedVariantKeys : [],
  };
  const hasRuntimeMeta =
    runtimeMeta.brand_source != null ||
    (Array.isArray(runtimeMeta.variant_keys_runtime) && runtimeMeta.variant_keys_runtime.length > 0);
  const docText = typeof processedText === 'string'
    ? processedText
    : (processedText != null ? String(processedText) : '');
  const normalizeSeedBrand = (value) => {
    if (value == null) return null;
    const trimmed = String(value).trim();
    if (!trimmed) return null;
    if (trimmed.toLowerCase() === 'unknown') return null;
    return trimmed;
  };
  const brandSeed =
    normalizeSeedBrand(processedEffective) ||
    normalizeSeedBrand(processedBrand) ||
    normalizeSeedBrand(processedDetected) ||
    null;
  const attachRuntimeMeta = (row) => {
    if (!hasRuntimeMeta) return;
    if (!row || typeof row !== 'object') return;
    row.raw_json = mergeRuntimeMetadata(row.raw_json, runtimeMeta);
  };
  if ((docText && docText.length) || brandSeed) {
    const applyRowHints = (row) => {
      if (!row || typeof row !== 'object') return;
      if (docText && (row._doc_text == null || row._doc_text === '')) {
        row._doc_text = docText;
      }
      if (brandSeed && (!row.brand || !String(row.brand).trim())) {
        row.brand = brandSeed;
      }
      attachRuntimeMeta(row);
    };
    for (const row of records) applyRowHints(row);
    if (Array.isArray(processedRowsInput) && processedRowsInput !== records) {
      for (const row of processedRowsInput) applyRowHints(row);
    }
  } else if (hasRuntimeMeta) {
    for (const row of records) attachRuntimeMeta(row);
    if (Array.isArray(processedRowsInput) && processedRowsInput !== records) {
      for (const row of processedRowsInput) attachRuntimeMeta(row);
    }
  }

  const runId = processed?.runId ?? processed?.run_id ?? overrides?.runId ?? overrides?.run_id ?? null;
  const jobId = processed?.jobId ?? processed?.job_id ?? overrides?.jobId ?? overrides?.job_id ?? null;

  let persistResult = { upserts: 0, written: [], skipped: [], warnings: [] };
  if (qualified && family && records.length) {
    const allowMinimal = String(process.env.ALLOW_MINIMAL_INSERT || '').trim() === '1';
    const requiredList = Array.isArray(requiredFields) ? requiredFields : [];
    const effectiveRequired = allowMinimal ? [] : requiredList;

    const safeBrand = (value) => {
      if (value == null) return null;
      const trimmed = String(value).trim();
      if (!trimmed) return null;
      if (trimmed.toLowerCase() === 'unknown') return null;
      return trimmed;
    };

    let brandOverride = safeBrand(overrides?.brand)
      || safeBrand(processedEffective)
      || safeBrand(processedBrand)
      || safeBrand(brandName)
      || safeBrand(extractedBrand)
      || safeBrand(processedDetected)
      || null;

    if (!brandOverride) {
      let baseName = '';
      try {
        const { name } = parseGcsUri(gcsUri || '');
        baseName = path.basename(name || '');
      } catch {}
      try {
        const guessed = await detectBrandFromText(docText || '', baseName);
        if (safeBrand(guessed)) brandOverride = guessed;
      } catch (err) {
        console.warn('[brand detect] persist retry failed:', err?.message || err);
      }
    }

    if (brandOverride) {
      for (const row of records) {
        if (!row || typeof row !== 'object') continue;
        const current = String(row.brand || '').trim();
        if (!current || current.toLowerCase() === 'unknown') {
          row.brand = brandOverride;
        }
      }
      if (Array.isArray(processedRowsInput) && processedRowsInput !== records) {
        for (const row of processedRowsInput) {
          if (!row || typeof row !== 'object') continue;
          const current = String(row.brand || '').trim();
          if (!current || current.toLowerCase() === 'unknown') {
            row.brand = brandOverride;
          }
        }
      }
    }

    let blueprint;
    let variantKeysSource = USE_VARIANT_KEYS && Array.isArray(processedVariantKeys)
      ? processedVariantKeys
      : null;
    if (USE_VARIANT_KEYS && (!Array.isArray(variantKeysSource) || !variantKeysSource.length) && family) {
      try {
        blueprint = await getBlueprint(family);
        if (!Array.isArray(variantKeysSource) || !variantKeysSource.length) {
          variantKeysSource = Array.isArray(blueprint?.ingestOptions?.variant_keys)
            ? blueprint.ingestOptions.variant_keys
            : null;
        }
      } catch (err) {
        console.warn('[persist] blueprint fetch failed for variant recovery:', err?.message || err);
      }
    }

    const variantKeys = USE_VARIANT_KEYS && Array.isArray(variantKeysSource)
      ? variantKeysSource.map((k) => String(k || '').trim()).filter(Boolean)
      : [];

    for (const r of records) {
      if (!r || typeof r !== 'object') continue;
      if (!Array.isArray(r.candidates) && Array.isArray(processed?.candidateCodes)) {
        r.candidates = processed.candidateCodes;
      }
      const fixed = recoverCode(r, { pnTemplate, variantKeys });
      if (fixed) {
        if (!r.code) r.code = fixed;
        if (!r.pn) r.pn = fixed;
      }
    }

    // 저장 직전 PN 정합성 강화
    records = records.filter((r) => {
      const pnRaw = String(r?.pn ?? '').trim();
      const codeRaw = String(r?.code ?? '').trim();
      if (pnRaw && /\{[^}]*\}/.test(pnRaw)) return false; // 템플릿 PN 컷
      if (codeRaw && /\{[^}]*\}/.test(codeRaw)) return false; // 템플릿 코드 컷
      const candidate = pnRaw || codeRaw;
      if (!candidate) return false;
      if (candidate.startsWith('pdf:')) return false; // PDF 앵커 토큰 컷
      return PN_STRICT.test(candidate); // 기본 포맷 검증
    });

    records = records.filter((r) => isValidCode(r?.pn || r?.code));
    if (!records.length) {
      persistResult.skipped = [{ reason: 'missing_pn' }];
    }

    if (records.length) {
      for (const r of records) {
        if (!r || typeof r !== 'object') continue;
        r.brand =
          safeBrand(r.brand) ||
          safeBrand(brandOverride) ||
          safeBrand(processedEffective) ||
          safeBrand(brandName) ||
          safeBrand(extractedBrand) ||
          safeBrand(processedDetected) ||
          null;

        if (!r.pn && r.code) r.pn = r.code;
        if (!r.code && r.pn) r.code = r.pn;

        if (r.pn != null && String(r.pn).trim() === '') r.pn = null;
        if (r.code != null && String(r.code).trim() === '') r.code = null;
        if (r.brand != null && String(r.brand).trim() === '') r.brand = null;
      }

      const mergeBrandFallback =
        safeBrand(brandOverride) ||
        safeBrand(processedEffective) ||
        safeBrand(processedBrand) ||
        safeBrand(brandName) ||
        safeBrand(extractedBrand) ||
        safeBrand(processedDetected) ||
        null;
      try {
        await mergeRecordsWithExisting({
          records,
          qualifiedTable: qualified,
          colTypes,
          brandFallback: mergeBrandFallback,
          family,
          coreKeys: effectiveRequired,
        });
      } catch (err) {
        console.warn('[merge] mergeRecordsWithExisting failed:', err?.message || err);
      }

      if (colTypes instanceof Map && colTypes.size) {
        for (const rec of records) {
          if (!rec || typeof rec !== 'object') continue;
          for (const [k, v] of Object.entries(rec)) {
            const keyLower = String(k || '').toLowerCase();
            const t = colTypes.get(keyLower) || colTypes.get(k);
            if (!t) continue;
            if (t === 'numeric')      rec[k] = coerceNumeric(v);
            else if (t === 'int')     rec[k] = toInt(v);
            else if (t === 'bool')    rec[k] = toBool(v);
          }
        }
      }
      console.log(
        '[DIAG] persist start table=%s family=%s records=%d required=%d',
        table,
        family,
        Array.isArray(records) ? records.length : -1,
        Array.isArray(effectiveRequired) ? effectiveRequired.length : -1,
      );
      if (Array.isArray(records) && records.length) {
        const r0 = records[0] || {};
        console.log('[PERSIST.INPUT]', {
          family,
          table,
          brand: r0.brand ?? null,
          pn: r0.pn ?? null,
          code: r0.code ?? null,
          display_name: r0.display_name ?? null,
        });
      }
      const schemaEnsureRows = Array.isArray(processedRowsInput) && processedRowsInput.length
        ? (processedRowsInput === records ? records : [...records, ...processedRowsInput])
        : records;

      if (family) {
        if (!blueprint) {
          try {
            blueprint = await getBlueprint(family);
          } catch (err) {
            console.warn('[persist] blueprint fetch failed for key widening:', err?.message || err);
          }
        }
        const knownList = Array.isArray(blueprint?.allowedKeys) ? [...blueprint.allowedKeys] : [];
        const knownLower = new Set(
          knownList.map((key) => String(key || '').trim().toLowerCase()).filter(Boolean),
        );
        const runtimeKeys = Array.from(gatherRuntimeSpecKeys(schemaEnsureRows));
        const unknownKeys = Array.from(
          new Set(
            runtimeKeys
              .map((key) => String(key || '').trim())
              .filter((key) => key && !knownLower.has(key.toLowerCase())),
          ),
        );
        if (unknownKeys.length) {
          try {
            const { map } = await aiCanonicalizeKeys(family, unknownKeys, knownList);
            const widened = new Set(knownList);
            for (const key of unknownKeys) {
              const rec = map?.[key] || {};
              let target = String(rec.canonical || '').trim();
              if (!target || rec.action !== 'map') target = key;
              const lower = target.toLowerCase();
              if (!lower || knownLower.has(lower)) continue;
              knownLower.add(lower);
              widened.add(target);
            }
            blueprint = blueprint && typeof blueprint === 'object' ? blueprint : {};
            blueprint.allowedKeys = Array.from(widened);
          } catch (err) {
            console.warn('[persist] aiCanonicalizeKeys failed:', err?.message || err);
          }
        }
      }

      if (Array.isArray(processedRowsInput) && processedRowsInput.length) {
        await ensureDynamicColumnsForRows(qualified, processedRowsInput);
      }
      await ensureDynamicColumnsForRows(qualified, schemaEnsureRows);
      await ensureDynamicColumnsForRows(qualified, records);
      try {
        persistResult = await saveExtractedSpecs({
          qualifiedTable: qualified,
          family,
          brand: brandOverride,
          records,
          pnTemplate,
          requiredKeys: effectiveRequired,
          coreSpecKeys: effectiveRequired,
          blueprint,
          runId,
          run_id: runId,
          jobId,
          job_id: jobId,
          gcsUri,
          orderingInfo: processed?.ordering_info,
          docType: processed?.doc_type,
        }) || persistResult;
      } catch (e) {
        console.warn('[persist] saveExtractedSpecs failed:', e?.message || e);
        if (!persistResult || typeof persistResult !== 'object') {
          persistResult = { upserts: 0, written: [], skipped: [], warnings: [] };
        }
        if (!Array.isArray(persistResult.warnings)) persistResult.warnings = [];
        persistResult.warnings.push(String(e?.message || e));
      }
    }
  } else if (!records.length) {
    persistResult.skipped = [{ reason: 'missing_pn' }];
  }

  const persistedCodes = new Set(
    (persistResult.written || [])
      .map((pn) => String(pn || '').trim())
      .filter(Boolean)
  );

  if (!persistedCodes.size && records.length) {
    for (const rec of records) {
      const pn = String(rec.pn || rec.code || '').trim();
      if (pn) persistedCodes.add(pn);
    }
  }

  const persistedList = Array.from(persistedCodes);
  const mpnListSafe = Array.isArray(mpnList) ? mpnList : [];
  const mergedMpns = Array.from(new Set([...persistedList, ...mpnListSafe]));

  const rejectReasons = new Set(
    (persistResult.skipped || [])
      .map((it) => (it && typeof it === 'object' ? it.reason : it))
      .filter(Boolean)
  );
  const warningReasons = new Set(
    (persistResult.warnings || []).filter(Boolean)
  );

  const ms = Number.isFinite(processed?.ms) ? processed.ms : (typeof started === 'number' ? Date.now() - started : null);
  const upsertsCount = typeof persistResult.upserts === 'number' ? persistResult.upserts : 0;
  const affected = typeof persistResult.affected === 'number' ? persistResult.affected : upsertsCount;
  const ok = affected > 0;

  const fallbackBrand = overrides.brand || brandName || processedEffective || extractedBrand || null;
  const primaryRecord = records[0] || null;
  const finalBrand = primaryRecord?.brand || fallbackBrand;
  const finalCode =
    persistedList[0] ||
    primaryRecord?.pn ||
    primaryRecord?.code ||
    overrides.code ||
    null;

  const response = {
    ok,
    ms,
    family,
    final_table: table,
    specs_table: table,
    brand: finalBrand,
    brand_effective: finalBrand,
    brand_source: processedBrandSource || null,
    code: finalCode,
    datasheet_uri: gcsUri,
    cover: coverUri || primaryRecord?.image_uri || null,
    rows: affected,        // 실제 반영된 개수만 기록
    codes: Array.from(persistedCodes),  // 표시는 그대로
    mpn_list: mergedMpns,
    variant_keys_runtime: runtimeMeta.variant_keys_runtime,
    reject_reasons: Array.from(rejectReasons),
    warnings: Array.from(warningReasons),
  };

  if (response.code == null && Array.isArray(processed?.candidateCodes) && processed.candidateCodes.length) {
    response.code = processed.candidateCodes[0];
  }

  if (response.code == null && baseSeries != null) {
    response.code = baseSeries;
  }

  response.affected = affected;
  if (typeof processed?.doc_type === 'string' && processed.doc_type) {
    response.doc_type = processed.doc_type;
  }
  return response;
}

module.exports = { runAutoIngest, persistProcessedData };
