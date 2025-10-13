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

const { pool } = tryRequire([
  path.join(__dirname, '../../db'),
  path.join(__dirname, '../db'),
  path.join(__dirname, './db'),
  path.join(process.cwd(), 'db'),
]);
const { ensureSpecsTable } = tryRequire([
  path.join(__dirname, '../utils/schema'),
  path.join(__dirname, '../../utils/schema'),
  path.join(__dirname, '../schema'),
  path.join(process.cwd(), 'schema'),
]);
const { getColumnsOf } = require('./ensure-spec-columns');
const { normalizeValueLLM } = require('../utils/ai');
let { renderPnTemplate: renderPnTemplateFromOrdering } = require('../utils/ordering');

const STRICT_CODE_RULES = /^(1|true|on)$/i.test(process.env.STRICT_CODE_RULES || '1');
const MIN_CORE_SPEC_COUNT = (() => {
  const raw = Number(process.env.MIN_CORE_SPEC_COUNT ?? 2);
  if (!Number.isFinite(raw) || raw <= 0) return 2;
  return Math.min(Math.floor(raw), 10);
})();

function norm(s) {
  return String(s || '')
    .replace(/[\s\-_/()]/g, '')
    .toUpperCase();
}

function codeForRelaySignal(spec) {
  const parts = [];
  const base = spec.pn || spec.series_code || spec.series || '';
  if (base) parts.push(base);

  const op = String(spec.operating_function || '').toLowerCase();
  if (op.includes('latch')) parts.push('L');

  const suf = (spec.suffix || '').trim();
  if (suf) parts.push(suf.toUpperCase());

  const cv = (spec.coil_voltage_vdc || spec.voltage || '')
    .toString()
    .replace(/\D/g, '');
  if (cv) parts.push(`DC${cv}V`);

  return parts
    .filter(Boolean)
    .join('-')
    .replace(/--+/g, '-');
}

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
  'last_error',
  'created_at',
  'updated_at',
]);

const CONFLICT_KEYS = ['brand', 'pn'];
const NEVER_INSERT = new Set(['id', 'brand_norm', 'code_norm', 'pn_norm', 'created_at', 'updated_at']);

const PN_RE = /\b[0-9A-Z][0-9A-Z\-_/().#]{3,63}[0-9A-Z)#]\b/i;
const FORBIDDEN_RE = /(pdf|font|xref|object|type0|ffff)/i;
const BANNED_PREFIX = /^(pdf|page|figure|table|sheet|rev|ver|draft)\b/i;
const BANNED_EXACT = /^pdf-?1(\.\d+)?$/i;

const RANGE_PATTERN = /(-?\d+(?:,\d{3})*(?:\.\d+)?)(?:\s*([kmgKMGmunpµ]))?(?:\s*[a-z%°]*)?\s*(?:to|~|–|—|-)\s*(-?\d+(?:,\d{3})*(?:\.\d+)?)(?:\s*([kmgKMGmunpµ]))?/i;
const NUMBER_PATTERN = /(-?\d+(?:,\d{3})*(?:\.\d+)?)(?:\s*([kmgKMGmunpµ]))?/i;
const SCALE_MAP = {
  k: 1e3,
    K: 1e3,
  m: 1e-3,
    M: 1e6,
  g: 1e9,
    G: 1e9,
  'µ': 1e-6,
  u: 1e-6,
  n: 1e-9,
  p: 1e-12,
};

const LLM_CONFIDENCE_THRESHOLD = (() => {
  const raw = Number(process.env.SPEC_AI_CONFIDENCE_MIN ?? process.env.SPEC_NORMALIZE_CONFIDENCE ?? 0.5);
  return Number.isFinite(raw) ? raw : 0.5;
})();

const llmNormalizationCache = new Map();

function buildBlueprintFieldMap(blueprint) {
  const map = new Map();
  if (!blueprint) return map;

  const raw =
    (blueprint.fields && typeof blueprint.fields === 'object' && blueprint.fields) ||
    blueprint.fields_json ||
    blueprint;

  if (Array.isArray(raw)) {
    for (const entry of raw) {
      if (!entry || typeof entry !== 'object') continue;
      const key = normKey(entry.name || entry.key || entry.field);
      if (!key) continue;
      map.set(key, entry);
    }
    return map;
  }

  if (raw && typeof raw === 'object') {
    for (const [name, meta] of Object.entries(raw)) {
      const key = normKey(name);
      if (!key) continue;
      if (meta && typeof meta === 'object') map.set(key, meta);
      else map.set(key, { type: meta });
    }
  }
  return map;
}

function buildAllowedKeySet(blueprint) {
  const allowed = Array.isArray(blueprint?.allowedKeys) ? blueprint.allowedKeys : [];
  const set = new Set();
  for (const key of allowed) {
    const norm = normKey(key);
    if (norm) set.add(norm);
  }
  return set;
}

function enumValuesFromMeta(meta) {
  if (!meta || typeof meta !== 'object') return null;
  if (Array.isArray(meta.enum) && meta.enum.length) return meta.enum;
  if (Array.isArray(meta.allowed) && meta.allowed.length) return meta.allowed;
  if (Array.isArray(meta.values) && meta.values.length) return meta.values;
  return null;
}

function makeNormalizationCacheKey({ family, key, raw, enumValues }) {
  const enumKey = Array.isArray(enumValues) && enumValues.length
    ? enumValues.map((v) => String(v ?? '').toLowerCase()).sort().join('|')
    : '';
  return [family || '', key || '', String(raw ?? ''), enumKey].join('::');
}

async function normalizeValueWithCache(params) {
  const cacheKey = makeNormalizationCacheKey(params);
  if (llmNormalizationCache.has(cacheKey)) {
    const cached = llmNormalizationCache.get(cacheKey);
    if (cached && typeof cached.then === 'function') {
      return cached;
    }
    return cached;
  }

  const task = normalizeValueLLM(params)
    .then((result) => {
      llmNormalizationCache.set(cacheKey, result);
      return result;
    })
    .catch(() => {
      const fallback = { normalized: null, confidence: 0, unit: null, magnitude: null };
      llmNormalizationCache.set(cacheKey, fallback);
      return fallback;
    });

  llmNormalizationCache.set(cacheKey, task);
  return task;
}

function normKey(key) {
  return String(key || '')
    .trim()
    .toLowerCase();
}

function isMinimalFallbackPn(value) {
  return typeof value === 'string' && value.startsWith('pdf:');
}

function isValidCode(value) {
  const trimmed = String(value || '').trim();
  if (!trimmed) return false;
  if (isMinimalFallbackPn(trimmed)) return false;
  if (/\{[^}]*\}/.test(trimmed)) return false;
  return PN_RE.test(trimmed);
}

function pickFiniteNumber(...candidates) {
  for (const candidate of candidates) {
    if (candidate == null || candidate === '') continue;
    const num = Number(candidate);
    if (Number.isFinite(num)) return num;
  }
  return null;
}

function normalizeDocType(value) {
  if (value == null) return null;
  const str = String(value).trim().toLowerCase();
  if (!str) return null;
  if (str.startsWith('order')) return 'ordering';
  if (str.startsWith('catalog') || str.startsWith('multi')) return 'catalog';
  if (str.startsWith('single') || str.startsWith('one')) return 'single';
  return null;
}

function normalizeOrderingInfoPayload(raw) {
  if (raw == null) return null;
  let value = raw;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return null;
    try {
      value = JSON.parse(trimmed);
    } catch (_) {
      return null;
    }
  }

  if (Array.isArray(value)) {
    const sections = value
      .map((entry) => normalizeOrderingInfoPayload(entry))
      .filter((entry) => entry && typeof entry === 'object');
    if (!sections.length) return null;
    if (sections.length === 1) return sections[0];
    return { sections };
  }

  if (!value || typeof value !== 'object') return null;

  const codes = [];
  const seenCodes = new Set();
  const collectCode = (input) => {
    const str = typeof input === 'string' ? input : String(input ?? '');
    const trimmed = str.trim();
    if (!trimmed) return;
    const normalized = trimmed.toUpperCase();
    if (seenCodes.has(normalized)) return;
    seenCodes.add(normalized);
    codes.push(normalized);
  };

  if (Array.isArray(value.codes)) {
    for (const code of value.codes) collectCode(code);
  }
  if (!codes.length && Array.isArray(value.scored)) {
    for (const entry of value.scored) {
      if (!entry || typeof entry !== 'object') continue;
      collectCode(entry.code);
    }
  }
  if (!codes.length && Array.isArray(value.sections)) {
    for (const section of value.sections) {
      const normalized = normalizeOrderingInfoPayload(section);
      if (!normalized) continue;
      if (Array.isArray(normalized.codes)) {
        for (const code of normalized.codes) collectCode(code);
      }
    }
  }

  if (!codes.length) return null;

  const textSources = [
    value.text,
    value.window_text,
    value.windowText,
    value?.window?.text,
  ];
  let text = null;
  for (const candidate of textSources) {
    if (typeof candidate === 'string' && candidate.trim()) {
      text = candidate;
      break;
    }
  }

  const start = pickFiniteNumber(value.start, value.window_start, value?.window?.start);
  const end = pickFiniteNumber(value.end, value.window_end, value?.window?.end);
  const anchorIndex = pickFiniteNumber(
    value.anchor_index,
    value.anchorIndex,
    value?.window?.anchor_index,
    value?.window?.anchorIndex,
  );

  const scored = Array.isArray(value.scored)
    ? value.scored
        .map((entry) => {
          if (!entry || typeof entry !== 'object') return null;
          const code = typeof entry.code === 'string' ? entry.code.trim().toUpperCase() : null;
          if (!code) return null;
          const score = Number(entry.score);
          const normalized = { code };
          if (Number.isFinite(score)) normalized.score = score;
          return normalized;
        })
        .filter(Boolean)
    : null;

  const payload = { codes };
  if (text) payload.text = text;
  if (start != null) payload.start = start;
  if (end != null) payload.end = end;
  if (anchorIndex != null) payload.anchor_index = anchorIndex;
  if (scored && scored.length) payload.scored = scored;

  return payload;
}

function repairPn(raw) {
  if (!raw) return null;
  let s = String(raw).trim();
  if (!s) return null;
  if (BANNED_PREFIX.test(s) || BANNED_EXACT.test(s)) return null;
  s = s.replace(/[–—―]/g, '-');
  s = s.replace(/\s+/g, '');
  s = s.replace(/[^0-9A-Za-z\-_/().#]/g, '');
  if (BANNED_PREFIX.test(s) || BANNED_EXACT.test(s)) return null;
  return s.length >= 3 ? s : null;
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
  const trimmedSuffix = String(suffix).trim();
  if (!trimmedSuffix) return base;
  const direct = SCALE_MAP[trimmedSuffix];
  if (direct != null) return base * direct;
  const scale = SCALE_MAP[trimmedSuffix.toLowerCase()];
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

async function applyAiNormalization({
  record,
  keys,
  columnTypes,
  fieldMetaMap,
  allowedKeySet,
  family,
}) {
  if (!record || typeof record !== 'object') return;
  if (!Array.isArray(keys) || !keys.length) return;

  for (const key of keys) {
    if (!key) continue;
    if (!Object.prototype.hasOwnProperty.call(record, key)) continue;
    if (allowedKeySet?.size && !allowedKeySet.has(key) && String(process.env.AUTO_ADD_FIELDS || '').trim() !== '1') continue;

    const original = record[key];
    if (original == null) continue;
    if (Array.isArray(original)) continue;
    if (typeof original === 'object') continue;
    if (typeof original === 'boolean') continue;
    if (typeof original === 'number' && !Number.isFinite(original)) continue;

    const type = columnTypes.get(key);
    const lowerType = String(type || '').toLowerCase();
    if (lowerType.includes('bool')) continue;

    if (typeof original === 'number' && isNumericType(type)) continue;

    const str = String(original).trim();
    if (!str) continue;

    const meta = fieldMetaMap.get(key);
    const enumValuesRaw = enumValuesFromMeta(meta);
    const enumCandidates = Array.isArray(enumValuesRaw) && enumValuesRaw.length
      ? enumValuesRaw.map((v) => String(v ?? '').trim()).filter(Boolean)
      : [];
    const enumValues = enumCandidates.length ? enumCandidates : null;

    try {
      const { normalized, confidence, magnitude } = await normalizeValueWithCache({
        family,
        key,
        raw: str,
        enumValues,
      });

      if (!Number.isFinite(confidence) || confidence < LLM_CONFIDENCE_THRESHOLD) continue;

      if (isNumericType(type) && typeof magnitude === 'number' && Number.isFinite(magnitude)) {
        record[key] = magnitude;
      } else if (normalized && typeof normalized === 'string' && normalized.trim()) {
        record[key] = normalized.trim();
      }
    } catch (_) {
      // Vertex 호출 실패는 치명적이지 않으므로 조용히 무시
    }
  }
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

const BRAND_LOOKUP_SQL = `
  SELECT brand_norm, brand
    FROM public.manufacturer_alias
   WHERE brand_norm = lower($1)
      OR lower($1) = ANY(aliases)
   LIMIT 1
`;

const BRAND_ALIAS_SCAN_SQL = `
  SELECT brand_norm, aliases
    FROM public.manufacturer_alias
`;

const brandCache = new Map();
let aliasRowsCache = null;
let aliasRowsFetchedAt = 0;

async function loadAliasRows() {
  const now = Date.now();
  if (aliasRowsCache && now - aliasRowsFetchedAt < 60_000) {
    return aliasRowsCache;
  }
  try {
    let { rows } = await pool.query(BRAND_ALIAS_SCAN_SQL);
    aliasRowsCache = Array.isArray(rows) ? rows : [];
    aliasRowsFetchedAt = now;
  } catch (err) {
    // Fallback: aliases 컬럼이 없으면 alias 단일값을 배열로 대체
    try {
      const { rows } = await pool.query(
        `SELECT brand_norm, alias FROM public.manufacturer_alias`
      );
      aliasRowsCache = rows.map((r) => ({
        brand_norm: String(r.brand_norm || '').toLowerCase(),
        aliases: r.alias ? [String(r.alias)] : [],
      }));
    } catch (e2) {
      aliasRowsCache = [];
      console.warn('[persist] alias scan fallback failed:', e2?.message || e2);
    }
    aliasRowsFetchedAt = now;
    console.warn('[persist] normalizeBrand alias scan failed:', err?.message || err);
  }
  return aliasRowsCache;
}

async function normalizeBrand(raw, docTextLower = '') {
  const trimmed = String(raw || '').trim();
  if (!trimmed) return null;
  const key = trimmed.toLowerCase();
  if (brandCache.has(key)) return brandCache.get(key);

  let resolved = null;
  try {
    const { rows } = await pool.query(BRAND_LOOKUP_SQL, [trimmed]);
    const row = rows?.[0];
    if (row?.brand_norm) {
      resolved = String(row.brand_norm).trim().toLowerCase();
    }
  } catch (err) {
    console.warn('[persist] normalizeBrand query failed:', err?.message || err);
  }

  const docLower = String(docTextLower || '').toLowerCase();
  if (!resolved && docLower) {
    const aliasRows = await loadAliasRows();
    const lowerRaw = key;
    for (const row of aliasRows) {
      const brandNorm = String(row?.brand_norm || '').trim().toLowerCase();
      if (!brandNorm) continue;
      const aliases = Array.isArray(row?.aliases) ? row.aliases : [];
      const rawMatch = lowerRaw.includes(brandNorm);
      const docMatch = docLower.includes(brandNorm) || aliases.some((alias) => {
        const lowerAlias = String(alias || '').trim().toLowerCase();
        return lowerAlias && docLower.includes(lowerAlias);
      });
      if (rawMatch || docMatch) {
        resolved = brandNorm;
        break;
      }
    }
  }

  if (resolved) {
    brandCache.set(key, resolved);
    brandCache.set(resolved, resolved);
  }
  return resolved;
}

function escapeRegExp(str) {
  return String(str || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function applyTemplateOptions(value, options = []) {
  if (value == null) return value;
  let out = String(value);
  for (const rawToken of options) {
    const token = String(rawToken || '').trim();
    if (!token) continue;
    const [opRaw, argRaw = ''] = token.split('=').map((t) => t.trim());
    const op = opRaw.toLowerCase();
    if (!op) continue;
    if (op === 'pad') {
      const width = Number(argRaw);
      if (Number.isFinite(width) && width > 0) out = out.padStart(width, '0');
      continue;
    }
    if (op === 'first') {
      out = out.split(',')[0].trim();
      continue;
    }
    if (op === 'alnum') {
      out = out.replace(/[^0-9A-Z]/gi, '');
      continue;
    }
    if (op === 'digits') {
      const m = out.match(/\d+/g) || [''];
      out = m.join('');
      continue;
    }
    if (op === 'upcase' || op === 'upper' || op === 'uppercase') {
      out = out.toUpperCase();
      continue;
    }
    if (op === 'downcase' || op === 'lower' || op === 'lowercase') {
      out = out.toLowerCase();
      continue;
    }
    if (op === 'trim') {
      out = out.trim();
      continue;
    }
    if (op === 'prefix') {
      out = `${argRaw}${out}`;
      continue;
    }
    if (op === 'suffix') {
      out = `${out}${argRaw}`;
      continue;
    }
    if (op === 'replace' && argRaw) {
      const [search, replacement = ''] = argRaw.split(':');
      if (search != null) {
        const matcher = new RegExp(escapeRegExp(search), 'g');
        out = out.replace(matcher, replacement);
      }
      continue;
    }
  }
  return out;
}

function looksLikeTemplate(value) {
  return typeof value === 'string' && value.includes('{') && value.includes('}');
}

function resolveTemplateValue(record, field) {
  const rawKey = String(field || '').trim();
  if (!rawKey) return null;
  const norm = normKey(rawKey);
  const candidates = [];
  const push = (key) => {
    if (!key) return;
    if (!candidates.includes(key)) candidates.push(key);
  };
  push(rawKey);
  if (norm && norm !== rawKey) push(norm);
  if (norm) {
    push(`${norm}_code`);
    push(`${norm}_text`);
    push(`${norm}_value`);
    push(`${norm}_raw`);
  }
  if (rawKey && rawKey !== norm) {
    push(`${rawKey}_code`);
    push(`${rawKey}_text`);
    push(`${rawKey}_value`);
    push(`${rawKey}_raw`);
  }

  const extract = (value) => {
    if (value == null) return null;
    if (Array.isArray(value)) {
      const first = value.find((v) => v != null && String(v).trim() !== '');
      return first != null ? first : null;
    }
    if (typeof value === 'object') {
      if ('value' in value && value.value != null && String(value.value).trim() !== '') {
        return value.value;
      }
      if ('text' in value && value.text != null && String(value.text).trim() !== '') {
        return value.text;
      }
      return null;
    }
    const str = String(value).trim();
    if (str === '') return null;
    return value;
  };

  for (const key of candidates) {
    if (!key) continue;
    const resolved = extract(record[key]);
    if (resolved != null) return resolved;
  }

  if (norm) {
    const normalizedOrder = [norm, `${norm}_code`, `${norm}_text`, `${norm}_value`, `${norm}_raw`];
    for (const target of normalizedOrder) {
      for (const [key, value] of Object.entries(record || {})) {
        if (normKey(key) !== target) continue;
        const resolved = extract(value);
        if (resolved != null) return resolved;
      }
    }
  }

  return null;
}

function renderTemplateWithPattern(template, record, pattern) {
  let used = false;
  const rendered = String(template).replace(pattern, (_, body) => {
    const parts = String(body || '')
      .split('|')
      .map((part) => part.trim())
      .filter(Boolean);
    if (!parts.length) return '';
    const base = parts.shift();
    const value = resolveTemplateValue(record, base);
    if (value == null || value === '') return '';
    used = true;
    const applied = applyTemplateOptions(String(value), parts);
    return applied == null ? '' : String(applied);
  });
  return { rendered, used };
}

function renderAnyTemplate(template, record = {}, ctxOrOptions = {}, maybeOptions = {}) {
  if (!template) return null;
  let ctxText = '';
  let options = { collapseWhitespace: true };
  if (typeof ctxOrOptions === 'string') {
    ctxText = ctxOrOptions;
    options = { ...options, ...(maybeOptions && typeof maybeOptions === 'object' ? maybeOptions : {}) };
  } else if (ctxOrOptions && typeof ctxOrOptions === 'object' && !Array.isArray(ctxOrOptions)) {
    options = { ...options, ...ctxOrOptions };
  }
  const context = record && typeof record === 'object' ? { ...record } : {};
  if (ctxText) {
    if (context._doc_text == null) context._doc_text = ctxText;
    if (context.doc_text == null) context.doc_text = ctxText;
    if (context.__text == null) context.__text = ctxText;
  }
  let working = String(template);
  let used = false;

  const double = renderTemplateWithPattern(working, context, /\{\{\s*([^{}]+?)\s*\}\}/g);
  working = double.rendered;
  if (double.used) used = true;

  const single = renderTemplateWithPattern(working, context, /\{\s*([^{}]+?)\s*\}/g);
  working = single.rendered;
  if (single.used) used = true;

  if (!used) return null;
  const collapseWhitespace = options?.collapseWhitespace !== false;
  let cleaned = collapseWhitespace ? working.replace(/\s+/g, '') : working;
  cleaned = cleaned.trim();
  return cleaned || null;
}

function renderPnTemplateLocal(template, record = {}) {
  return renderAnyTemplate(template, record);
}

const renderPnTemplate =
  typeof renderPnTemplateFromOrdering === 'function' ? renderPnTemplateFromOrdering : renderPnTemplateLocal;

function buildPnIfMissing(record = {}, pnTemplate) {
  const existing = String(record.pn || '').trim();
  if (existing) return;
  const fromTemplate = renderPnTemplate(pnTemplate, record);
  // 본문 검증: 템플릿 결과가 실제 문서 텍스트에 존재할 때만 채택
  const ctxText = String(record._doc_text || record.doc_text || '');
  if (fromTemplate && ctxText) {
    const normalizedContext = norm(ctxText);
    const normalizedTemplate = norm(fromTemplate);
    if (normalizedTemplate && normalizedContext.includes(normalizedTemplate)) {
      record.pn = fromTemplate;
      if (!record.code) record.code = fromTemplate;
      return;
    }
  }
  const code = String(record.code || '').trim();
  if (code) record.pn = code;
}

function buildBestIdentifiers(family, spec = {}, blueprint) {
  if (!spec || typeof spec !== 'object') return spec;

  let codeCandidate = null;
  const localTemplate = blueprint?.pn_template || spec?._pn_template || null;
  if (localTemplate) {
    try {
      codeCandidate = renderPnTemplate(localTemplate, spec);
    } catch (_) {}
  }

  const docText = String(spec._doc_text || spec.doc_text || '');
  if (!codeCandidate && family === 'relay_signal') {
    const fallback = codeForRelaySignal(spec);
    if (fallback && norm(docText).includes(norm(fallback))) {
      codeCandidate = fallback;
    }
  }
  if (codeCandidate && norm(docText).includes(norm(codeCandidate))) {
    spec.pn = codeCandidate;
    spec.code = codeCandidate;
    spec.verified_in_doc = true;
  } else {
    spec.code = spec.pn;
    if (!STRICT_CODE_RULES) spec._warn_invalid_code = true;
  }

    if (Object.prototype.hasOwnProperty.call(spec, '_pn_template')) {
    delete spec._pn_template;
  }

  return spec;
}

function hasCoreSpecValue(value) {
  if (value == null) return false;
  if (Array.isArray(value)) return value.some((v) => hasCoreSpecValue(v));
  if (typeof value === 'boolean') return true;
  if (typeof value === 'number') return Number.isFinite(value);
  const str = String(value).trim();
  return Boolean(str);
}

function hasCoreSpec(row, keys = [], candidateKeys = []) {
  if (!row || typeof row !== 'object') return false;
  const requiredCount = MIN_CORE_SPEC_COUNT;
  const primary = Array.isArray(keys) ? keys.filter(Boolean) : [];
  const secondary = Array.isArray(candidateKeys) ? candidateKeys.filter(Boolean) : [];
  const seen = new Set();

  const testKey = (key) => {
    if (!key) return false;
    const norm = normKey(key);
    if (!norm || seen.has(norm) || META_KEYS.has(norm)) return false;
    const direct = row[norm];
    const value = direct !== undefined ? direct : row[key];
    if (hasCoreSpecValue(value)) {
      seen.add(norm);
      return true;
    }
    return false;
  };

  let count = 0;
  for (const key of primary) {
    if (testKey(key)) {
      count += 1;
      if (count >= requiredCount) return true;
    }
  }

  if (count < requiredCount) {
    for (const key of secondary) {
      if (testKey(key)) {
        count += 1;
        if (count >= requiredCount) return true;
      }
    }
  }

  if (count < requiredCount && primary.length === 0 && secondary.length === 0) {
    for (const [rawKey, rawValue] of Object.entries(row)) {
      const norm = normKey(rawKey);
      if (!norm || META_KEYS.has(norm) || seen.has(norm)) continue;
      if (hasCoreSpecValue(rawValue)) {
        seen.add(norm);
        count += 1;
        if (count >= requiredCount) return true;
      }
    }
  }

  return count >= requiredCount;
}

function isMinimalInsertEnabled() {
  return false;
}

function shouldInsert(row, { coreSpecKeys, candidateSpecKeys } = {}) {
  if (!row || typeof row !== 'object') {
    return { ok: false, reason: 'empty_row' };
  }

  const brand = String(row.brand || '').trim().toLowerCase();
  if (!brand || brand === 'unknown') {
    row.last_error = 'missing_brand';
    return { ok: false, reason: 'missing_brand' };
  }

  let pn = String(row.pn || row.code || '').trim();
  const allowMinimal = isMinimalInsertEnabled();
  const docType = String(row.doc_type || '').trim().toLowerCase();
  let verified = row.verified_in_doc;
  if (typeof verified === 'string') {
    verified = verified.trim().toLowerCase() === 'true';
  } else {
    verified = Boolean(verified);
  }
  row.verified_in_doc = verified;
  if (!verified) {
    row.last_error = row.last_error || 'unverified_in_doc';
    return { ok: false, reason: 'unverified_in_doc' };
  }
  if (!isValidCode(pn)) {
    const rawPn = String(row.pn || '').trim();
    const rawCode = String(row.code || '').trim();
    if ((rawPn && /\{[^}]*\}/.test(rawPn)) || (rawCode && /\{[^}]*\}/.test(rawCode))) {
      row.last_error = 'invalid_code_template_placeholder';
      return { ok: false, reason: 'invalid_code' };
    }
    const fixed = repairPn(pn);
    if (fixed && isValidCode(fixed)) {
      console.warn('[persist] pn repaired', { original: pn, fixed });
      row.last_error = row.last_error || 'invalid_code_fixed';
      pn = fixed;
    } else if (allowMinimal) {
      const fallbackPn = repairPn(String(row.series || row.code || ''));
      if (fallbackPn && isValidCode(fallbackPn)) {
        console.warn('[persist] pn fallback applied', { original: pn, fallback: fallbackPn });
        pn = fallbackPn;
        row.last_error = row.last_error || 'invalid_code_fallback';
      } else {
        row.last_error = 'invalid_code';
        return { ok: false, reason: 'invalid_code' };
      }
    } else {
      row.last_error = 'invalid_code';
      return { ok: false, reason: 'invalid_code' };
    }
  } else {
    if (FORBIDDEN_RE.test(pn) || BANNED_PREFIX.test(pn) || BANNED_EXACT.test(pn)) {
      const fixed = repairPn(pn);
      if (fixed && isValidCode(fixed) && !FORBIDDEN_RE.test(fixed) && !BANNED_PREFIX.test(fixed) && !BANNED_EXACT.test(fixed)) {
        console.warn('[persist] pn repaired', { original: pn, fixed });
        row.last_error = row.last_error || 'invalid_code_fixed';
        pn = fixed;
      } else {
        row.last_error = 'invalid_code';
        return { ok: false, reason: 'invalid_code' };
      }
    }
  }
  row.pn = pn;
  if (row.code == null || String(row.code).trim() === '') row.code = pn;
  const hasCore = hasCoreSpec(row, coreSpecKeys, candidateSpecKeys);
  if (!hasCore) {
    row.last_error = row.last_error || 'missing_core_spec';
    return { ok: false, reason: 'missing_core_spec' };
  }
  return { ok: true };
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

async function ensureSchemaGuards(familySlug, targetTable) {
  if (!familySlug) return { ok: true };
  try {
    await pool.query('SELECT public.ensure_specs_table($1)', [familySlug]);
  } catch (err) {
    console.warn('[schema] ensure_specs_table failed (persist):', err?.message || err);
    if (targetTable) {
      try {
        await ensureSpecsTable(targetTable);
      } catch (fallbackErr) {
        return { ok: false, reason: 'schema_not_ready', detail: fallbackErr?.message || String(fallbackErr) };
      }
    } else {
      return { ok: false, reason: 'schema_not_ready', detail: err?.message || String(err) };
    }
  }
  try {
    await pool.query('SELECT public.ensure_blueprint_variant_columns($1)', [familySlug]);
  } catch (err) {
    return { ok: false, reason: 'schema_not_ready', detail: err?.message || String(err) };
  }
  return { ok: true };
}

async function saveExtractedSpecs(targetTable, familySlug, rows = [], options = {}) {
    if (targetTable && typeof targetTable === 'object' && !Array.isArray(targetTable)) {
    const params = targetTable;
    const actualTable =
      params.qualifiedTable ||
      params.qualified ||
      params.table ||
      params.targetTable ||
      null;
    const actualRows = Array.isArray(params.records)
      ? params.records
      : Array.isArray(params.rows)
        ? params.rows
        : rows;
    const actualFamily =
      params.family ||
      params.familySlug ||
      params.family_slug ||
      familySlug ||
      null;
    const nextOptions = { ...params };
    delete nextOptions.qualifiedTable;
    delete nextOptions.qualified;
    delete nextOptions.table;
    delete nextOptions.targetTable;
    delete nextOptions.family;
    delete nextOptions.familySlug;
    delete nextOptions.family_slug;
    delete nextOptions.records;
    delete nextOptions.rows;
    targetTable = actualTable;
    familySlug = actualFamily;
    rows = actualRows;
    options = { ...nextOptions };
  }

  const result = { processed: 0, upserts: 0, affected: 0, written: [], skipped: [], warnings: [] };
  if (!rows.length) return result;

  console.log(`[PATH] persist family=${familySlug} rows=${rows.length} brand_override=${options?.brand || ''}`);

  const gcsUri = options?.gcsUri || options?.gcs_uri || null;

  const runId = options?.runId ?? options?.run_id ?? null;
  const jobId = options?.jobId ?? options?.job_id ?? null;
  const suffixParts = [];
  if (runId) suffixParts.push(`run:${runId}`);
  if (jobId) suffixParts.push(`job:${jobId}`);
  const appNameSuffix = suffixParts.length ? ` ${suffixParts.join(' ')}` : '';

  const guard = await ensureSchemaGuards(familySlug, targetTable);
  if (!guard.ok) {
    result.skipped.push({ reason: guard.reason || 'schema_not_ready', detail: guard.detail || null });
    return result;
  }

  const physicalCols = await getColumnsOf(targetTable);
  if (!physicalCols.size) {
    result.skipped.push({ reason: 'schema_not_ready' });
    return result;
  }

  if (!physicalCols.has('pn') || !physicalCols.has('brand')) {
    result.skipped.push({ reason: 'schema_not_ready' });
    return result;
  }

  const columnTypes = await getColumnTypes(targetTable);
  const blueprintMeta = options?.blueprint || null;
  const blueprintFieldMap = buildBlueprintFieldMap(blueprintMeta);
  const blueprintAllowedSet = buildAllowedKeySet(blueprintMeta);
  const pnTemplate = typeof options.pnTemplate === 'string' && options.pnTemplate ? options.pnTemplate : null;
  const sharedOrderingInfo = normalizeOrderingInfoPayload(options?.orderingInfo);
  const sharedDocType = normalizeDocType(options?.docType);
  const requiredKeys = Array.isArray(options.requiredKeys)
    ? options.requiredKeys.map((k) => normKey(k)).filter(Boolean)
    : [];
  const explicitCoreKeys = Array.isArray(options.coreSpecKeys)
    ? options.coreSpecKeys.map((k) => normKey(k)).filter(Boolean)
    : [];
  const guardKeys = explicitCoreKeys.length ? explicitCoreKeys : requiredKeys;
  let candidateSpecKeys = [];

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
  candidateSpecKeys = colList.filter((key) => !META_KEYS.has(key) && key !== 'raw_json');

  const insertCols = colList.filter((col) => !NEVER_INSERT.has(col));
  const placeholders = insertCols.map((_, i) => `$${i + 1}`).join(',');

  const updateCols = insertCols.filter((col) => !CONFLICT_KEYS.includes(col));
  const updateSql = updateCols.length
    ? updateCols.map((col) => `"${col}" = EXCLUDED."${col}"`).join(', ') + `, "updated_at" = now()`
    : null;

  // Spec tables enforce uniqueness via the expression index (lower(brand), lower(pn)).
  // In production this exists only as an expression index, so we must always
  // reference the raw expression instead of a constraint name.
  const conflict = 'ON CONFLICT ((lower(brand)), (lower(pn)))';

  const sql = [
    `INSERT INTO ${targetTable} (${insertCols.map((c) => `"${c}"`).join(',')})`,
    `VALUES (${placeholders})`,
    conflict,
    updateSql ? `DO UPDATE SET ${updateSql}` : 'DO NOTHING',
    'RETURNING pn',
  ].join('\n');

  const client = await pool.connect();
  const warnings = new Set();
  const seenNatural = new Set();

  try {
    for (const [rowIndex, row] of rows.entries()) {
      result.processed += 1;
      const rec = {};
      for (const [key, value] of Object.entries(row || {})) {
        rec[normKey(key)] = value;
      }

      if (options?.brand) {
        rec.brand = options.brand;
      }

      const docTextRaw = String(
        rec._doc_text ??
          rec.doc_text ??
          row?._doc_text ??
          row?.doc_text ??
          (options?.docText || options?.doc_text) ??
          '',
      );
      const docTextLower = docTextRaw.toLowerCase();
      const brandCandidates = [options?.brand, rec.brand, rec.brand_norm];
      let brandKey = null;
      for (const candidate of brandCandidates) {
        if (!candidate) continue;
        const trimmed = String(candidate).trim();
        if (!trimmed) continue;
        brandKey = await normalizeBrand(trimmed, docTextLower);
        if (brandKey) break;
      }
      if (brandKey) {
        if (!rec.brand || !String(rec.brand).trim()) {
          rec.brand = options?.brand || brandKey;
        }
        rec.brand_norm = brandKey;
      } else if (physicalCols.has('brand_norm')) {
        rec.brand_norm = null;
      }
      if (physicalCols.has('last_error')) rec.last_error = null;

      let orderingPayload = sharedOrderingInfo;
      if (Object.prototype.hasOwnProperty.call(rec, 'ordering_info') && rec.ordering_info != null) {
        orderingPayload = normalizeOrderingInfoPayload(rec.ordering_info) || orderingPayload;
      }

      let docTypeValue = sharedDocType;
      if (Object.prototype.hasOwnProperty.call(rec, 'doc_type')) {
        const normalizedRowDocType = normalizeDocType(rec.doc_type);
        if (normalizedRowDocType) docTypeValue = normalizedRowDocType;
      }

      if (orderingPayload || docTypeValue) {
        let rawHolder = rec.raw_json;
        if (typeof rawHolder === 'string') {
          try {
            rawHolder = JSON.parse(rawHolder);
          } catch (_) {
            rawHolder = {};
          }
        }
        if (!rawHolder || typeof rawHolder !== 'object' || Array.isArray(rawHolder)) {
          rawHolder = {};
        }
        if (orderingPayload && !rawHolder.ordering_info) {
          let cloned = orderingPayload;
          try {
            cloned = JSON.parse(JSON.stringify(orderingPayload));
          } catch (_) {}
          rawHolder.ordering_info = cloned;
        }
        if (docTypeValue && !rawHolder.doc_type) {
          rawHolder.doc_type = docTypeValue;
        }
        rec.raw_json = rawHolder;
      }
      if (Object.prototype.hasOwnProperty.call(rec, 'ordering_info')) {
        delete rec.ordering_info;
      }
      if (Object.prototype.hasOwnProperty.call(rec, 'doc_type')) {
        delete rec.doc_type;
      }

      const templateContext = { ...rec };
      const ctxText = docTextRaw;
      const pnWasTemplate = looksLikeTemplate(templateContext.pn);
      const codeWasTemplate = looksLikeTemplate(templateContext.code);

      if (pnWasTemplate) {
        const renderedPn = renderAnyTemplate(templateContext.pn, templateContext, ctxText);
        rec.pn = renderedPn ?? null;
      }

      if (codeWasTemplate) {
        const contextForCode = { ...templateContext, pn: rec.pn ?? templateContext.pn };
        const renderedCode = renderAnyTemplate(templateContext.code, contextForCode, ctxText);
        rec.code = renderedCode ?? null;
      }

      if (!isValidCode(rec.pn) && isValidCode(rec.code)) {
        rec.pn = rec.code;
      }

      if (!isValidCode(rec.pn)) {
        rec.pn = null;
      }

      if (!rec.pn && rec.code) {
        rec.pn = rec.code;
      }

      buildPnIfMissing(rec, pnTemplate);

      buildBestIdentifiers(familySlug, rec, blueprintMeta);
      if (!STRICT_CODE_RULES && rec._warn_invalid_code) {
        console.warn(
          '[WARN] invalid_code (soft) family=%s pn=%s code=%s',
          familySlug,
          rec.pn,
          rec.code,
        );
      }

      if (!isValidCode(rec.pn) && isValidCode(rec.code)) {
        rec.pn = rec.code;
      }

      // 🔹 템플릿 미치환 차단: 아직 { } 가 남아있다면 유효 PN 아님
      if (looksLikeTemplate(rec.pn) || looksLikeTemplate(rec.code)) {
        if (physicalCols.has('last_error')) rec.last_error = 'template_unresolved';
        result.skipped.push({ reason: 'invalid_code', detail: 'template_unresolved' });
        continue;
      }

      const pnMissing = !isValidCode(rec.pn);
      if (pnMissing && (pnWasTemplate || codeWasTemplate)) {
        if (physicalCols.has('last_error')) rec.last_error = 'template_render_failed';
        result.skipped.push({ reason: 'invalid_code', detail: 'template_render_failed' });
        continue;
      }

      if (!isValidCode(rec.code) && isValidCode(rec.pn)) {
        rec.code = rec.pn;
      }

      const guard = shouldInsert(rec, { coreSpecKeys: guardKeys, candidateSpecKeys });
      if (!guard.ok) {
        const skip = { reason: guard.reason, detail: guard.detail || null };
        if (rec.last_error) skip.last_error = rec.last_error;
        result.skipped.push(skip);
        continue;
      }

      const pnValue = String(rec.pn || rec.code || '').trim();
      const pnIsFallback = isMinimalFallbackPn(pnValue);
      if (!pnValue || !isValidCode(pnValue) || (!pnIsFallback && FORBIDDEN_RE.test(pnValue))) {
        const skippedCode = pnValue || String(rec.code || rec.pn || '').trim() || '(no-code)';
        if (physicalCols.has('last_error')) rec.last_error = 'invalid_code';
        result.skipped.push({ reason: 'invalid_code', code: skippedCode, last_error: 'invalid_code' });
        continue;
      }

      rec.pn = pnValue;
      if (rec.code == null || String(rec.code).trim() === '') {
        rec.code = pnValue;
      }

      const pnNorm = normKey(pnValue);
      if (!pnNorm) {
        if (physicalCols.has('last_error')) rec.last_error = 'missing_pn';
        result.skipped.push({ reason: 'missing_pn', last_error: 'missing_pn' });
        continue;
      }
      if (physicalCols.has('pn_norm')) rec.pn_norm = pnNorm;

      const codeNorm = normKey(rec.code);
      if (!codeNorm) {
        if (physicalCols.has('last_error')) rec.last_error = 'invalid_code';
        result.skipped.push({ reason: 'invalid_code', last_error: 'invalid_code' });
        continue;
      }
      if (physicalCols.has('code_norm')) rec.code_norm = codeNorm;

      const brandNatural = normKey(rec.brand) || rec.brand_norm || '';
      const naturalKey = `${brandNatural}::${pnNorm}`;
      if (seenNatural.has(naturalKey)) {
        result.skipped.push({ reason: 'duplicate_code' });
        continue;
      }
      seenNatural.add(naturalKey);

      rec.family_slug = familySlug;

      const display = rec.display_name || `${rec.brand} ${rec.pn}`;
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

      if (candidateSpecKeys.length) {
        await applyAiNormalization({
          record: rec,
          keys: candidateSpecKeys,
          columnTypes,
          fieldMetaMap: blueprintFieldMap,
          allowedKeySet: blueprintAllowedSet,
          family: familySlug,
        });
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

      const vals = insertCols.map((col) => {
        if (col === 'raw_json') return sanitized.raw_json ?? null;
        return sanitized[col] ?? null;
      });

      try {
        await client.query('BEGIN');
        if (appNameSuffix) {
          await client.query(
            `SELECT set_config('application_name', current_setting('application_name', true) || $1, true)`,
            [appNameSuffix],
          );
        }
        const res = await client.query(sql, vals);
        await client.query('COMMIT');
        const delta = res.rowCount || 0;
        result.upserts += delta;
        result.affected += delta;
        if (res.rows?.[0]?.pn) result.written.push(res.rows[0].pn);
      } catch (err) {
        await client.query('ROLLBACK').catch(() => {});
        result.skipped.push({ reason: 'db_error', detail: err?.message || String(err) });
      }
    }
  } finally {
    client.release();
  }

  if (result.affected > 0 && options?.refreshViews !== false) {
    const refresh = {};
    try {
      await pool.query('SELECT public.refresh_component_specs_view()');
      refresh.component_specs_view = { ok: true };
    } catch (err) {
      refresh.component_specs_view = { ok: false, error: err?.message || String(err) };
      warnings.add('refresh_component_specs_view_failed');
    }
    try {
      await pool.query('SELECT retail.refresh_products_src_view()');
      refresh.products_src_view = { ok: true };
    } catch (err) {
      refresh.products_src_view = { ok: false, error: err?.message || String(err) };
      warnings.add('refresh_products_src_view_failed');
    }
    result.refresh = refresh;
  }

  result.warnings = Array.from(warnings);
  return result;
}

module.exports = { saveExtractedSpecs, looksLikeTemplate, renderAnyTemplate };
