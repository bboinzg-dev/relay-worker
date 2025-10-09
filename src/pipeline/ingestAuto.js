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
const { explodeToRows, splitAndCarryPrefix } = require('../utils/mpn-exploder');
const { ensureSpecColumnsForBlueprint } = require('./ensure-spec-columns');
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

const META_KEYS = new Set(['variant_keys','pn_template','ingest_options']);
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

const PN_CANDIDATE_RE = /[0-9A-Z][0-9A-Z\-_/().]{3,63}[0-9A-Z)]/gi;
const PN_BLACKLIST_RE = /(pdf|font|xref|object|type0|ffff)/i;

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
  return true;
}

const KEY_ALIASES = {
  form: ['form', 'contact_form', 'contact_arrangement', 'configuration', 'arrangement', 'poles_form'],
  voltage: ['voltage', 'coil_voltage_vdc', 'voltage_vdc', 'rated_voltage_vdc', 'vdc', 'coil_voltage'],
  case: ['case', 'case_code', 'package', 'pkg'],
  capacitance: ['capacitance', 'capacitance_uF', 'capacitance_f', 'c'],
  resistance: ['resistance', 'resistance_ohm', 'r_ohm', 'r'],
  tolerance: ['tolerance', 'tolerance_pct'],
  length_mm: ['length_mm', 'dim_l_mm'],
  width_mm: ['width_mm', 'dim_w_mm'],
  height_mm: ['height_mm', 'dim_h_mm'],
  series: ['series', 'series_code']
};

const ENUM_MAP = {
  form: { SPST: '1A', SPDT: '1C', DPDT: '2C' }
};

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
      const match = String(value).match(/(\d)\s*form\s*([ABC])/i);
      if (match) {
        value = `${match[1]}${match[2].toUpperCase()}`;
      }
    }

    if (!value && ctxText) {
      const match = ctxText.match(/(\d)\s*form\s*([ABC])/i);
      if (match) {
        value = `${match[1]}${match[2].toUpperCase()}`;
      } else {
        const poleMatch = ctxText.match(/\b(SPST|SPDT|DPDT)\b/i);
        if (poleMatch) value = poleMatch[1].toUpperCase();
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
  if (advanced != null && advanced !== '') return advanced;

  let out = pnTemplate;
  const dict = new Map(Object.entries(rec || {}));
  for (const k of variantKeys) if (!dict.has(k)) dict.set(k, rec?.[k]);
  dict.set('series', rec?.series ?? rec?.series_code ?? '');
  dict.set('series_code', rec?.series_code ?? rec?.series ?? '');
  out = out.replace(/\{([a-z0-9_]+)\}/ig, (_, k) => String(dict.get(k) ?? ''));
  out = out.replace(/\s+/g,'').trim();
  return out || null;
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
    if (ser && norm && !norm.startsWith(ser)) continue;
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

function applyCodeRules(code, out, rules, colTypes) {
  if (!Array.isArray(rules)) return;
  const src = String(code || '');
  for (const r of rules) {
    const re = new RegExp(r.pattern, r.flags || 'i');
    const m = src.match(re);
    if (!m) continue;
    for (const [col, spec] of Object.entries(r.set || {})) {
      if (!colTypes.has(col)) continue;
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
function resolveDocTypeFromExtraction(payload, text = '') {
  if (!payload || typeof payload !== 'object') return null;

  const existingRaw = typeof payload.doc_type === 'string' ? payload.doc_type.trim().toLowerCase() : '';
  const orderingCodes = Array.isArray(payload?.ordering_info?.codes)
    ? payload.ordering_info.codes.filter(Boolean).length
    : 0;

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

  let inferred = 'single';
  if (orderingCodes > 0) inferred = 'ordering';
  else if (rowCodes.size > 1 || candidateCodes.size > 1) inferred = 'catalog';

  const haystack = String(text || '').toLowerCase();
  if (inferred !== 'ordering' && orderingCodes === 0 && haystack) {
    if (
      haystack.includes('how to order') ||
      haystack.includes('ordering information') ||
      haystack.includes('ordering info') ||
      haystack.includes('주문') ||
      haystack.includes('订购') ||
      haystack.includes('订货')
    ) {
      inferred = 'ordering';
    }
  }
  if (inferred === 'single' && candidateCodes.size > 0 && haystack) {
    if (
      haystack.includes('catalog') ||
      haystack.includes('product list') ||
      haystack.includes('types') ||
      haystack.includes('part no') ||
      haystack.includes('part number')
    ) {
      inferred = 'catalog';
    }
  }

  if (existingRaw === 'ordering') return 'ordering';
  if (existingRaw === 'catalog') {
    return inferred === 'ordering' ? 'ordering' : 'catalog';
  }
  if (existingRaw === 'single') {
    if (inferred === 'ordering') return 'ordering';
    if (inferred === 'catalog') return 'catalog';
    return 'single';
  }

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
      const r = await extractText(gcsUri);
      previewText = r?.text || previewText;
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

  const pnTemplate = USE_PN_TEMPLATE
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
      const r = await extractText(gcsUri);
      previewText = r?.text || previewText;
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
  let extracted = { brand: brandHint || 'unknown', rows: [] };
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
          extracted = {
            brand: fallbackBrand,
            rows: [{ brand: fallbackBrand, code: code || (path.parse(fileName).name), ...(vals||{}) }],
          };
        } else {
          // 스캔/이미지형 PDF 등 텍스트가 없으면 정밀 추출을 1회만 하드캡으로 시도
          extracted = await withTimeout(
            extractPartsAndSpecsFromPdf({ gcsUri, allowedKeys, family, brandHint }),
            EXTRACT_HARD_CAP_MS,
            'extract',
          );
        }
      } else {
        extracted = await withTimeout(
          extractPartsAndSpecsFromPdf({ gcsUri, allowedKeys, family, brandHint }),
          EXTRACT_HARD_CAP_MS,
          'extract',
        );
      }
    } catch (e) { console.warn('[extract timeout/fail]', e?.message || e); }
  }

  if (docAiText) {
    if (!extracted || typeof extracted !== 'object') extracted = {};
    const existing = typeof extracted.text === 'string' ? extracted.text : '';
    if (!existing || docAiText.length > existing.length) {
      extracted.text = docAiText;
    }
  }
  if (docAiTables.length) {
    if (!extracted || typeof extracted !== 'object') extracted = {};
    if (!Array.isArray(extracted.tables) || !extracted.tables.length) {
      extracted.tables = docAiTables;
    }
  }
  if (vertexExtractValues && typeof vertexExtractValues === 'object') {
    const entries = Object.entries(vertexExtractValues);
    if (entries.length) {
      if (!Array.isArray(extracted.rows) || !extracted.rows.length) {
        extracted.rows = [{ ...vertexExtractValues }];
      } else {
        for (const row of extracted.rows) {
          if (!row || typeof row !== 'object') continue;
          for (const [rawKey, rawValue] of entries) {
            const key = String(rawKey || '').trim();
            if (!key) continue;
            if (row[key] == null || row[key] === '') {
              row[key] = rawValue;
            }
          }
        }
      }
    }
  }
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
    extracted.codes = list;        // <- 최종 MPN 배열
    extracted.mpn_list = list;     // <- 동의어(외부에서 쓰기 쉽도록)
  }

  if (!code && !codes.length) {
    let fullText = '';
    try { fullText = await readText(gcsUri, 300 * 1024) || ''; } catch {}

    const fromTypes  = extractPartNumbersFromTypesTables(fullText, FIRST_PASS_CODES * 4); // TYPES 표 우선
    const fromOrder  = rankPartNumbersFromOrderingSections(fullText, FIRST_PASS_CODES);
    const fromSeries = extractPartNumbersBySeriesHeuristic(fullText, FIRST_PASS_CODES * 4);
    console.log(`[PATH] pns={tables:${fromTypes.length}, body:${fromOrder.length}} combos=0`);
    // 가장 신뢰 높은 순서로 병합
    const picks = fromTypes.length ? fromTypes : (fromOrder.length ? fromOrder : fromSeries);

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
          const uniq = Array.from(new Set([...(extracted.codes || []), ...merged]));
          extracted.codes = uniq;
          extracted.mpn_list = uniq;
        }
      }
    }

    // 분할 여부는 별도 판단. 여기서는 후보만 모아둠.
    // extracted.rows는 건드리지 않음.
  }

  const docTypeResolved = resolveDocTypeFromExtraction(
    extracted,
    extracted?.text || previewText || ''
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

  let runtimeVariantKeys = [];
  if (USE_VARIANT_KEYS) {
    try {
      runtimeVariantKeys = await detectVariantKeys({
        rawText: extractedText,
        family,
        blueprintVariantKeys: variantKeys,
      });
    } catch (err) {
      console.warn('[variant] runtime detect failed:', err?.message || err);
      runtimeVariantKeys = Array.isArray(variantKeys) ? [...variantKeys] : [];
    }
  }

  console.log('[PATH] brand resolved', {
    runId,
    family,
    hint: brandHintSeed || null,
    effective: brandEffectiveResolved,
    source: brandSource,
    vkeys_runtime: runtimeVariantKeys,
  });


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
      const { detected: inferredKeys = [], newKeys: freshKeys = [] } = await inferVariantKeys({
        family,
        brand: brandName,
        series: baseSeries,
        blueprint,
        extracted,
      });

      if (Array.isArray(inferredKeys) && inferredKeys.length) {
        const brandSlug = normalizeSlug(brandName);
        const seriesSlug = normalizeSlug(baseSeries);
        try {
          await db.query(
            `SELECT public.upsert_variant_keys($1,$2,$3,$4::jsonb)`,
            [family, brandSlug, seriesSlug, JSON.stringify(inferredKeys)],
          );
        } catch (err) {
          console.warn('[variant] upsert_variant_keys failed:', err?.message || err);
        }

        const mergedVariant = new Set(variantKeys);
        for (const key of inferredKeys) mergedVariant.add(key);
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

        if (!disableEnsure) {
          try {
            await ensureBlueprintVariantColumns(family);
            variantColumnsEnsured = true;
          } catch (err) {
            console.warn('[variant] ensure_blueprint_variant_columns failed:', err?.message || err);
          }
        }
      }

      if (Array.isArray(freshKeys) && freshKeys.length) {
        console.log('[variant] detected new keys', { family, brand: brandName, series: baseSeries, keys: freshKeys });
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

  const rawRows = Array.isArray(extracted.rows) && extracted.rows.length ? extracted.rows : [];
  const runtimeSpecKeys = gatherRuntimeSpecKeys(rawRows);

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
        const limitRaw = Number(process.env.AUTO_ADD_FIELDS_LIMIT || '20');
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

  const explodedRows = USE_CODE_RULES ? explodeToRows(blueprint, baseRows) : baseRows;
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
  };

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
    const allowMinimal = /^(1|true|on)$/i.test(process.env.ALLOW_MINIMAL_INSERT || '0');
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

    records = records.filter((r) => isValidCode(r?.pn || r?.code));
    if (!records.length) {
      persistResult.skipped = [{ reason: 'missing_pn' }];
    }

    if (records.length) {
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
      persistResult = await saveExtractedSpecs(qualified, family, records, {
        brand: brandOverride,
        pnTemplate,
        requiredKeys: effectiveRequired,
        coreSpecKeys: effectiveRequired,
        blueprint,
        runId,
        run_id: runId,
        jobId,
        job_id: jobId,
        gcsUri,
        orderingInfo: extracted?.ordering_info,
        docType: extracted?.doc_type,
      }) || persistResult;
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
  if (typeof extracted?.doc_type === 'string' && extracted.doc_type) {
    response.doc_type = extracted.doc_type;
  }
  return response;
}

module.exports = { runAutoIngest, persistProcessedData };
