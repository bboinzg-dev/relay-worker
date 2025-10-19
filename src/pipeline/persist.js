// @ts-check
/// <reference path="../types/blueprint.d.ts" />
'use strict';

const path = require('node:path');
const tryRequire = require('../utils/try-require');

const { pool } = tryRequire([
  path.join(__dirname, '../../db'),
  path.join(__dirname, '../db'),
  path.join(__dirname, './db'),
  path.join(process.cwd(), 'db'),
]);
// bring normalizer for contact_form synonyms (e.g., "DPDT" → "2C")
let normalizeContactForm = (value) => value;
try {
  ({ normalizeContactForm } = require('../utils/mpn-exploder'));
} catch (e) {
  // optional
}
const { ensureSpecsTable } = tryRequire([
  path.join(__dirname, '../utils/schema'),
  path.join(__dirname, '../../utils/schema'),
  path.join(__dirname, '../schema'),
  path.join(process.cwd(), 'schema'),
]);
const { getColumnsOf } = require('./ensure-spec-columns');
const { normalizeValueLLM } = require('../utils/ai');
let { renderPnTemplate: renderPnTemplateFromOrdering } = require('../utils/ordering');
const { isValidCode } = require('../utils/code-validation');
const { getBlueprintPnTemplate } = require('../utils/getBlueprintPnTemplate');
let extractOrderingInfo;
try {
  ({ extractOrderingInfo } = require('../utils/ordering-sections'));
} catch (e) {
  // optional
}
let collectPnCandidates;
try {
  ({ collectPnCandidates } = require('../utils/pn-candidates'));
} catch (e) {
  // optional
}
let normalizeSpecKey = (value) => {
  if (value == null) return '';
  let str = String(value || '').trim();
  if (!str) return '';
  let prefix = '';
  const leading = str.match(/^_+/);
  if (leading) {
    prefix = leading[0];
    str = str.slice(prefix.length);
  }
  if (!str) return prefix.toLowerCase();
  const camelConverted = str.replace(/([a-z0-9])([A-Z])/g, '$1_$2');
  const sanitized = camelConverted.replace(/[^a-zA-Z0-9_]/g, '_');
  const collapsed = sanitized.replace(/__+/g, '_').replace(/^_+|_+$/g, '');
  const final = collapsed ? `${prefix}${collapsed}` : prefix;
  return final.toLowerCase();
};
try {
  ({ normalizeSpecKey } = require('../utils/key-normalize'));
} catch (e) {
  // optional fallback retains legacy behaviour
}

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
  if (cv) parts.push(`DC${cv}${/H$/i.test(suf) ? 'H' : ''}`); // 'V' 대신 접미 'H'까지 고려

  return parts
    .filter(Boolean)
    .join('-')
    .replace(/--+/g, '-');
}

function looksLikeGarbageCode(value) {
  const text = String(value ?? '');
  if (!text) return false;
  return (
    /^[a-f0-9]{20,}_\d{10,}/i.test(text)
    || /(^|_)(mech|doc|pdf)[-_]/i.test(text)
    || /pdf:|\.pdf$/i.test(text)
    || /^ASCTB\d{3,4}[A-Z]$/i.test(text)          // Panasonic catalog doc-id
    || /ASCTB\d{3,4}[A-Z]\s+\d{6}/i.test(text)    // with trailing yyyymm
  );
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
const NEVER_INSERT = new Set([
  'id','brand_norm','code_norm','pn_norm','created_at','updated_at',
  // 본문/검증 보조 필드는 DB에 저장하지 않음
  'text','doc_text','_doc_text'
]);

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

  const assign = (sourceKey, value) => {
    const normed = normalizeSpecKey(sourceKey) || normKey(sourceKey);
    if (!normed) return;
    map.set(normed, value);
  };

  if (Array.isArray(raw)) {
    for (const entry of raw) {
      if (!entry || typeof entry !== 'object') continue;
      const sourceKey = entry.name || entry.key || entry.field;
      assign(sourceKey, entry);
    }
    return map;
  }

  if (raw && typeof raw === 'object') {
    for (const [name, meta] of Object.entries(raw)) {
      const value = meta && typeof meta === 'object' ? meta : { type: meta };
      assign(name, value);
    }
  }
  return map;
}

function buildAllowedKeySet(blueprint) {
  const allowed = Array.isArray(blueprint?.allowedKeys) ? blueprint.allowedKeys : [];
  const set = new Set();
  for (const key of allowed) {
    const norm = normalizeSpecKey(key) || normKey(key);
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

function normalizeCodeCandidateList(list) {
  if (!Array.isArray(list) || !list.length) return [];
  const normalized = [];
  const seen = new Set();
  for (const entry of list) {
    if (entry == null) continue;
    let raw = entry;
    if (typeof entry === 'object' && entry && Object.prototype.hasOwnProperty.call(entry, 'code')) {
      raw = entry.code;
    }
    if (typeof raw !== 'string') {
      if (raw == null) continue;
      raw = String(raw);
    }
    const trimmed = raw.trim();
    if (!trimmed) continue;
    const upper = trimmed.toUpperCase();
    if (seen.has(upper)) continue;
    seen.add(upper);
    normalized.push(upper);
  }
  return normalized;
}

function buildCandidateCodeSet(...lists) {
  const set = new Set();
  for (const list of lists) {
    const normalized = normalizeCodeCandidateList(list);
    for (const code of normalized) set.add(code);
  }
  return set.size ? set : null;
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

function toCandidateArray(input) {
  if (!input) return [];
  if (Array.isArray(input)) return input;
  if (input instanceof Set) return Array.from(input);
  return [input];
}

function normalizeCandidatePnValue(raw) {
  if (raw == null) return null;
  if (Array.isArray(raw)) {
    for (const entry of raw) {
      const normalized = normalizeCandidatePnValue(entry);
      if (normalized) return normalized;
    }
    return null;
  }
  if (typeof raw === 'object') {
    if (raw && typeof raw.code === 'string') return normalizeCandidatePnValue(raw.code);
    if (raw && typeof raw.value === 'string') return normalizeCandidatePnValue(raw.value);
    if (raw && typeof raw.text === 'string') return normalizeCandidatePnValue(raw.text);
    if (raw && typeof raw.pn === 'string') return normalizeCandidatePnValue(raw.pn);
    return null;
  }
  let str = String(raw || '').trim();
  if (!str) return null;
  if (looksLikeTemplate(str)) return null;
  if (looksLikeGarbageCode(str)) return null;
  const repaired = repairPn(str) || str.replace(/\s+/g, '');
  const cleaned = String(repaired || '').trim();
  if (!cleaned) return null;
  if (!isValidCode(cleaned)) return null;
  return cleaned;
}

function collectCandidateEntries(sources = []) {
  const entries = new Map();
  const list = Array.isArray(sources) ? sources : [];
  for (const source of list) {
    if (!source) continue;
    const {
      values,
      priority = 10,
      requireDocHit = false,
      allowNoDoc = false,
      fallbackVerified = false,
    } = source;
    const label = source.source || source.label || 'candidate';
    const rawValues = [];
    for (const value of toCandidateArray(values)) {
      if (value == null) continue;
      if (Array.isArray(value)) rawValues.push(...value);
      else rawValues.push(value);
    }
    if (!rawValues.length) continue;
    const seenLocal = new Set();
    const normalizedList = [];
    for (const raw of rawValues) {
      const normalized = normalizeCandidatePnValue(raw);
      if (!normalized) continue;
      const upper = normalized.toUpperCase();
      if (seenLocal.has(upper)) continue;
      seenLocal.add(upper);
      normalizedList.push({ value: normalized, upper });
    }
    if (!normalizedList.length) continue;
    const allowFallback = allowNoDoc || (!requireDocHit && normalizedList.length === 1);
    for (const candidate of normalizedList) {
      const existing = entries.get(candidate.upper);
      const next = {
        value: candidate.value,
        upper: candidate.upper,
        priority,
        requireDocHit,
        allowNoDoc: allowFallback,
        fallbackVerified: allowFallback && fallbackVerified,
        source: label,
      };
      if (!existing || next.priority < existing.priority) {
        entries.set(candidate.upper, next);
      } else if (existing && next.priority === existing.priority && candidate.value.length > existing.value.length) {
        entries.set(candidate.upper, { ...existing, value: candidate.value });
      }
    }
  }
  return Array.from(entries.values());
}

function selectBestCandidate(entries, docText) {
  if (!Array.isArray(entries) || !entries.length) return null;
  const haystack = typeof docText === 'string' ? docText : String(docText ?? '');
  const docMatches = [];
  const fallbacks = [];
  for (const entry of entries) {
    if (!entry || !entry.value) continue;
    const inDoc = haystack && typeof fuzzyContainsPn === 'function' && fuzzyContainsPn(haystack, entry.value);
    if (inDoc) {
      docMatches.push(entry);
      continue;
    }
    if (!entry.requireDocHit && entry.allowNoDoc) {
      fallbacks.push(entry);
    }
  }
  if (docMatches.length) {
    docMatches.sort((a, b) => a.priority - b.priority || b.value.length - a.value.length);
    const chosen = docMatches[0];
    return { value: chosen.value, verified: true, source: chosen.source };
  }
  if (fallbacks.length) {
    fallbacks.sort((a, b) => a.priority - b.priority || b.value.length - a.value.length);
    const chosen = fallbacks[0];
    return { value: chosen.value, verified: Boolean(chosen.fallbackVerified), source: chosen.source };
  }
  return null;
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

function fuzzyContainsPn(text, pn) {
  const hay = typeof text === 'string' ? text : String(text ?? '');
  const raw = String(pn || '').trim();
  if (!raw || !hay) return false;
  // 영문/숫자/그 외 기호로 토큰화 후, 토큰 사이에는 항상 [-\s]* 허용
  // 유니코드 대시(– — ―)와 수학용 마이너스(−)까지 허용하도록 확장
  const glue = '[-–—―−\\s]*';
  const toks = raw.match(/[A-Za-z]+|\d+|[^A-Za-z0-9]+/g) || [];
  const piece = toks
    .map((t) => (/^[A-Za-z0-9]+$/.test(t)
      ? t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
      : glue))
    .join(glue);
  // 'V' vs 'VDC' 허용 + 끝에 붙는 접미 문자 앞 공백 허용
  const pattern = piece.replace(/V$/i, 'V(?:DC)?') + '(?:\\s*[A-Z])?$';
  const re = new RegExp(`(^|[^A-Za-z0-9])${pattern}`, 'i');
  return re.test(hay);
}

function buildPnIfMissing(record = {}, pnTemplate) {
    // (1) 본문 텍스트 하이드레이션: raw_json.docai.text → _doc_text/doc_text
  try {
    if (!record._doc_text && !record.doc_text && record.raw_json) {
      const obj = typeof record.raw_json === 'string' ? JSON.parse(record.raw_json) : record.raw_json;
      const t = obj?.docai?.text || null;
      if (t) {
        record._doc_text = t;
        record.doc_text = t;
      }
    }
  } catch (err) {
    // ignore malformed raw_json payloads
  }

  const existing = String(record.pn || '').trim();
  if (existing) return;
  const fromTemplate = renderPnTemplate(pnTemplate, record);
  // 본문 검증: 템플릿 결과가 실제 문서 텍스트에 존재할 때만 채택
  const ctxText = String(record._doc_text || record.doc_text || record.text || '');
  if (fromTemplate && ctxText && fuzzyContainsPn(ctxText, fromTemplate)) {
    record.pn = fromTemplate;
    if (!record.code) record.code = fromTemplate;
    return;
  }
  const code = String(record.code || '').trim();
  if (code) record.pn = code;
}

// (2) 어디서 들어왔든 pn/code가 본문에 있으면 verified_in_doc 보강
function verifyInDocIfPresent(record = {}) {
  if (record.verified_in_doc) return;
  const docText = String(record._doc_text || record.doc_text || record.text || '');
  if (!docText) return;
  let orderingInfo = record._ordering_info || record.ordering_info || record.orderingInfo || null;
  const store = (value) => {
    if (!value || typeof value !== 'object') return;
    orderingInfo = { ...(orderingInfo && typeof orderingInfo === 'object' ? orderingInfo : {}), ...value };
    record._ordering_info = orderingInfo;
    record.ordering_info = orderingInfo;
    record.orderingInfo = orderingInfo;
  };
  if (!orderingInfo && typeof extractOrderingInfo === 'function') {
    const parsed = extractOrderingInfo(docText, 200);
    if (parsed) store(parsed);
  }
  if (!orderingInfo) return;

  const textSource = orderingInfo.text;
  const orderingText = Array.isArray(textSource) ? textSource.join('\n') : String(textSource || '');
  const candidates = [];
  if (record.pn) candidates.push(record.pn);
  if (record.code && record.code !== record.pn) candidates.push(record.code);
  const validCandidates = candidates.filter((cand) => isValidCode(cand) && !looksLikeGarbageCode(cand));
  if (!validCandidates.length) return;

  const codes = Array.isArray(orderingInfo.codes) ? orderingInfo.codes : null;
  if (codes && codes.length) {
    const codeSet = new Set(
      codes
        .map((c) => String(c || '').trim().toUpperCase())
        .filter((c) => c && isValidCode(c) && !looksLikeGarbageCode(c)),
    );
    if (codeSet.size) {
      for (const cand of validCandidates) {
        if (codeSet.has(String(cand).trim().toUpperCase())) {
          record.verified_in_doc = true;
          return;
        }
      }
    }
  }

  if (!orderingText) return;
  for (const cand of validCandidates) {
    if (typeof fuzzyContainsPn === 'function' && fuzzyContainsPn(orderingText, cand)) {
      record.verified_in_doc = true;
      return;
    }
    if (norm(orderingText).includes(norm(cand))) {
      record.verified_in_doc = true;
      return;
    }
  }
}

/**
 * @param {string} family
 * @param {import('../types/blueprint').Spec} spec
 * @param {import('../types/blueprint').Blueprint} [blueprint]
 */
function buildBestIdentifiers(family, spec = {}, blueprint) {
  if (!spec || typeof spec !== 'object') return spec;

  // ① 문서 본문 텍스트 확보 (DocAI 텍스트 → _doc_text/doc_text)
  try {
    const raw = spec.raw_json;
    const obj = typeof raw === 'string' ? JSON.parse(raw) : raw;
    const docaiText = obj?.docai?.text || '';
    if (docaiText) {
      if (!spec._doc_text) spec._doc_text = docaiText;
      if (!spec.doc_text) spec.doc_text = docaiText;
    }
  } catch (_) {}

  let codeCandidate = null;
  const docText = String(spec._doc_text || spec.doc_text || '');
  let orderingInfo = spec._ordering_info || spec.ordering_info || spec.orderingInfo || null;
  const storeOrderingInfo = (value) => {
    if (!value || typeof value !== 'object') return;
    const base = orderingInfo && typeof orderingInfo === 'object' ? orderingInfo : {};
    orderingInfo = { ...base, ...value };
    spec.ordering_info = orderingInfo;
    spec.orderingInfo = orderingInfo;
    spec._ordering_info = orderingInfo;
  };
  if (orderingInfo && typeof orderingInfo === 'object') {
    // ensure shared reference for downstream consumers
    storeOrderingInfo(orderingInfo);
  }
  let docai = null;
  try {
    const raw = spec.raw_json;
    const parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;
    if (parsed && typeof parsed === 'object') {
      docai = parsed.docai || parsed.doc_ai || docai;
    }
  } catch (err) {
    // ignore malformed raw_json payloads
  }
  if (!docai && spec && typeof spec === 'object' && spec.docai) {
    docai = spec.docai;
  }
  if (!orderingInfo && docText && typeof extractOrderingInfo === 'function') {
    const parsed = extractOrderingInfo(docText, 200);
    if (parsed) storeOrderingInfo(parsed);
  }
  if ((!orderingInfo || !Array.isArray(orderingInfo.codes) || orderingInfo.codes.length === 0)
      && typeof collectPnCandidates === 'function') {
    const res = collectPnCandidates({ docText, docai, extractOrderingInfo });
    if (res && Array.isArray(res.codes) && res.codes.length) {
      storeOrderingInfo({ codes: res.codes });
    }
  }

  const localTemplate = getBlueprintPnTemplate(blueprint || {}, spec);
  if (localTemplate) {
    try {
      const context = { ...spec };
      if (orderingInfo && typeof orderingInfo === 'object') {
        context.ordering_info = orderingInfo;
        context.orderingInfo = orderingInfo;
        if (context.codes == null && Array.isArray(orderingInfo.codes)) {
          context.codes = orderingInfo.codes;
        }
      }
      const rendered = renderPnTemplate(localTemplate, context);
      if (rendered) {
        codeCandidate = rendered;
      }
    } catch (_) {
      // ignore template rendering errors for identifier derivation
    }
  }

  if (!codeCandidate && family === 'relay_signal') {
    const fallback = codeForRelaySignal(spec);
    if (fallback && norm(docText).includes(norm(fallback))) {
      codeCandidate = fallback;
    }
  }
  let docHit = false;
  const candidate = String(codeCandidate || '').trim();
  if (candidate && isValidCode(candidate) && !looksLikeGarbageCode(candidate)) {
    const candidateUpper = candidate.toUpperCase();
    const codes = Array.isArray(orderingInfo?.codes) ? orderingInfo.codes : null;
    if (codes && codes.length) {
      docHit = codes.some((c) => {
        const cc = String(c || '').trim();
        if (!cc || !isValidCode(cc) || looksLikeGarbageCode(cc)) return false;
        return cc.toUpperCase() === candidateUpper;
      });
    }
    if (!docHit) {
      const textSource = orderingInfo?.text;
      const orderingText = Array.isArray(textSource) ? textSource.join('\n') : String(textSource || '');
      if (orderingText) {
        docHit = typeof fuzzyContainsPn === 'function'
          ? fuzzyContainsPn(orderingText, candidate)
          : norm(orderingText).includes(norm(candidate));
      }
    }
  }
  if (candidate && docHit) {
    spec.pn = codeCandidate;
    spec.code = codeCandidate;
    spec.verified_in_doc = true;
  }
  // 유효한 쪽을 보존: 한쪽만 살아있으면 상호 보완하고,
  // 둘 다 있으면 아무 것도 덮어쓰지 않는다.
  const pnOk = isValidCode(spec.pn) && !looksLikeGarbageCode(spec.pn);
  const codeOk = isValidCode(spec.code) && !looksLikeGarbageCode(spec.code);
  if (!codeOk && pnOk) spec.code = spec.pn;
  if (!pnOk && codeOk) spec.pn = spec.code;

  if (!spec.verified_in_doc) {
    const codes = Array.isArray(orderingInfo?.codes) ? orderingInfo.codes : null;
    if (codes && codes.length) {
      const me = String(spec.pn || spec.code || '').trim().toUpperCase();
      if (me && isValidCode(me) && !looksLikeGarbageCode(me)) {
        const matched = codes.some((c) => {
          const cc = String(c || '').trim().toUpperCase();
          if (!cc) return false;
          if (!isValidCode(cc) || looksLikeGarbageCode(cc)) return false;
          return cc === me;
        });
        if (matched) spec.verified_in_doc = true;
      }
    }
  }

  if (!STRICT_CODE_RULES) {
    const finalPnOk = isValidCode(spec.pn) && !looksLikeGarbageCode(spec.pn);
    const finalCodeOk = isValidCode(spec.code) && !looksLikeGarbageCode(spec.code);
    if (!finalPnOk && !finalCodeOk && (spec.pn || spec.code)) {
      spec._warn_invalid_code = true;
    } else if (spec._warn_invalid_code && (finalPnOk || finalCodeOk)) {
      delete spec._warn_invalid_code;
    }
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
  const primary = Array.isArray(keys)
    ? keys.map((key) => normalizeSpecKey(key) || normKey(key)).filter(Boolean)
    : [];
  const secondary = Array.isArray(candidateKeys)
    ? candidateKeys.map((key) => normalizeSpecKey(key) || normKey(key)).filter(Boolean)
    : [];
  const seen = new Set();

  const testKey = (key) => {
    if (!key) return false;
    const norm = normalizeSpecKey(key) || normKey(key);
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
  return /^(1|true|on)$/i.test(process.env.ALLOW_MINIMAL_INSERT || '0');
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
    const ord = row._ordering_info;
    const ordList = Array.isArray(ord?.codes) ? ord.codes : [];
    const ordHit =
      isValidCode(pn) &&
      ordList.some((code) => String(code ?? '').trim().toUpperCase() === pn.toUpperCase());
    if (ordHit) {
      verified = true;
      row.verified_in_doc = true;
    }
  }
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
     if (docType === 'ordering' || allowMinimal) {
      return { ok: true };
    }src/pipeline/persist.js

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
  const sharedMpnSet = buildCandidateCodeSet(options?.mpnList, options?.mpn_list);
  const normalizeKeyInput = (value) => normalizeSpecKey(value) || normKey(value);
  const requiredKeys = Array.isArray(options.requiredKeys)
    ? options.requiredKeys.map(normalizeKeyInput).filter(Boolean)
    : [];
  const explicitCoreKeys = Array.isArray(options.coreSpecKeys)
    ? options.coreSpecKeys.map(normalizeKeyInput).filter(Boolean)
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
      const normalized = normalizeSpecKey(key) || normKey(key);
      if (!normalized) continue;
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

      const originalPnValue = resolveTemplateValue(row, 'pn');
      const originalCodeValue = resolveTemplateValue(row, 'code');
      const originalPnString =
        originalPnValue == null
          ? ''
          : String(originalPnValue).trim();
      const originalCodeString =
        originalCodeValue == null
          ? ''
          : String(originalCodeValue).trim();
      const originalPnIsTemplate = looksLikeTemplate(originalPnString);
      const originalCodeIsTemplate = looksLikeTemplate(originalCodeString);
      const originalPnIsValid =
        !originalPnIsTemplate && isValidCode(originalPnString);
      const originalCodeIsValid =
        !originalCodeIsTemplate && isValidCode(originalCodeString);

      const rec = {};
      for (const [key, value] of Object.entries(row || {})) {
        const norm = normKey(key);
        const specKey = normalizeSpecKey(key) || norm;
        let target = specKey;
        if (specKey && physicalCols.has(specKey)) {
          target = specKey;
        } else if (norm && physicalCols.has(norm)) {
          target = norm;
        } else if (!specKey && norm) {
          target = norm;
        }

        if (target) rec[target] = value;
        if (norm && norm !== target && !Object.prototype.hasOwnProperty.call(rec, norm)) {
          rec[norm] = value;
        }
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
      
      const verificationOrderingInfo =
        orderingPayload ??
        (Object.prototype.hasOwnProperty.call(rec, 'ordering_info') ? rec.ordering_info : null) ??
        null;
      if (verificationOrderingInfo) {
        rec._ordering_info = verificationOrderingInfo;
      } else if (Object.prototype.hasOwnProperty.call(rec, '_ordering_info')) {
        delete rec._ordering_info;
      }
      // buildBestIdentifiers/shouldInsert가 ordering 정보를 참조하므로 이 단계에서는 유지한다.
      // (필요시 INSERT 직전에 정리하도록 한다.)
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

            if (!isValidCode(rec.pn) && originalPnIsValid) {
        rec.pn = originalPnString;
      }

      if (!isValidCode(rec.code) && (originalCodeIsValid || originalPnIsValid)) {
        rec.code = originalCodeIsValid ? originalCodeString : originalPnString;
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
      verifyInDocIfPresent(rec);
      
      // 🔎 Fallback verification:
      // ORDERING 추출이 실패하더라도 본문 텍스트에 pn/code가 실제로 존재하면 verified_in_doc 인정
      if (!rec.verified_in_doc) {
        const hay = String(docTextRaw || '');
        if ((rec.pn && fuzzyContainsPn(hay, rec.pn)) || (rec.code && fuzzyContainsPn(hay, rec.code))) {
          rec.verified_in_doc = true;
        }
      }

      buildBestIdentifiers(familySlug, rec, blueprintMeta);
      // 본문에 실제 PN/코드가 보이면 verified_in_doc = true
      if (!rec.verified_in_doc) {
        const hay = String(rec._doc_text || rec.doc_text || rec.text || '');
        const hasPn = rec.pn && fuzzyContainsPn && fuzzyContainsPn(hay, rec.pn);
        const hasCode = !hasPn && rec.code && fuzzyContainsPn && fuzzyContainsPn(hay, rec.code);
        if (hasPn || hasCode) rec.verified_in_doc = true;
      }
      // ◾ top-level key normalization to avoid duplicate columns like "terminal form"
      const renames = [];
      for (const key of Object.keys(rec)) {
        const normalizedKey = normalizeSpecKey(key);
        if (!normalizedKey || normalizedKey === key) continue;
        if (Object.prototype.hasOwnProperty.call(rec, normalizedKey)) continue;
        renames.push([key, normalizedKey]);
      }
      for (const [from, to] of renames) {
        rec[to] = rec[from];
        delete rec[from];
      }
      // ◾ contact_form synonyms → contact_form
      if (!rec.contact_form) {
        const cf = normalizeContactForm(
          rec.contact_form ?? rec.contact_arrangement ?? rec.configuration ?? null,
        );
        if (cf) rec.contact_form = cf;
      }
      // ◾ terminal_shape -> mount_type heuristic
      if (!rec.mount_type && rec.terminal_shape) {
        const t = String(rec.terminal_shape).toLowerCase();
        if (/pc\s*pin|through|dip/.test(t)) rec.mount_type = 'Through-Hole';
      }
      // ◾ poles만 온 경우 보조 추론
      if (!rec.contact_form && Number.isFinite(rec.poles)) {
        if (Number(rec.poles) === 2) rec.contact_form = '2C';
        else if (Number(rec.poles) === 1) rec.contact_form = '1C';
      }
      // ◾ packing_style은 텍스트로 유지(“50 pcs” 숫자 오인 방지)
      if (typeof rec.packing_style === 'number') {
        rec.packing_style = String(rec.packing_style);
      }

      if (!rec.verified_in_doc) {
        if (rec.code && !isValidCode(rec.code) && looksLikeGarbageCode(rec.code)) {
          rec.code = null;
        }
        if (rec.pn && !isValidCode(rec.pn) && looksLikeGarbageCode(rec.pn)) {
          rec.pn = null;
        }
      }
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

      const rowMpnSet = buildCandidateCodeSet(
        row?.mpn_list,
        row?.mpnList,
        rec?.mpn_list,
        rec?.mpnList,
      );
      if (!rec.verified_in_doc && rowMpnSet && rowMpnSet.size) {
        const me = String(rec.pn || rec.code || '').trim().toUpperCase();
        if (me && rowMpnSet.has(me)) {
          rec.verified_in_doc = true;
        }
      }
            if (!isValidCode(rec.pn) && !isValidCode(rec.code)) {
        const orderingCandidates = [];
        if (Array.isArray(orderingPayload?.codes)) orderingCandidates.push(...orderingPayload.codes);
        if (Array.isArray(orderingPayload?.scored)) {
          for (const scored of orderingPayload.scored) {
            if (!scored || typeof scored !== 'object') continue;
            if (typeof scored.code === 'string') orderingCandidates.push(scored.code);
          }
        }

        const candidateKeys = [
          'pn_candidates',
          'candidate_pns',
          'candidate_codes',
          'candidates',
          'codes',
          'code_list',
          'codeList',
          'mpn_candidates',
          'mpnCandidates',
        ];
        const rowCandidateValues = [];
        for (const key of candidateKeys) {
          const value = Object.prototype.hasOwnProperty.call(row || {}, key) ? row[key] : null;
          if (value == null) continue;
          if (Array.isArray(value)) rowCandidateValues.push(...value);
          else rowCandidateValues.push(value);
        }
        if (Array.isArray(rec?.candidates)) rowCandidateValues.push(...rec.candidates);
        if (Array.isArray(options?.candidates)) rowCandidateValues.push(...options.candidates);

        const sharedLists = [];
        if (Array.isArray(options?.mpnList)) sharedLists.push(...options.mpnList);
        if (Array.isArray(options?.mpn_list)) sharedLists.push(...options.mpn_list);

        const candidateEntries = collectCandidateEntries([
          orderingCandidates.length
            ? { values: orderingCandidates, source: 'ordering', priority: 0, fallbackVerified: true }
            : null,
          rowCandidateValues.length
            ? { values: rowCandidateValues, source: 'row', priority: 1 }
            : null,
          rowMpnSet && rowMpnSet.size
            ? { values: Array.from(rowMpnSet), source: 'row_mpn', priority: 1 }
            : null,
          sharedMpnSet && sharedMpnSet.size
            ? { values: Array.from(sharedMpnSet), source: 'shared_mpn', priority: 2, requireDocHit: true }
            : null,
          sharedLists.length
            ? { values: sharedLists, source: 'shared_list', priority: 2, requireDocHit: true }
            : null,
        ]);

        const pick = selectBestCandidate(candidateEntries, docTextRaw);
        if (pick && pick.value) {
          rec.pn = pick.value;
          if (!rec.code) rec.code = pick.value;
          if (!rec.verified_in_doc && pick.verified) {
            rec.verified_in_doc = true;
          }
          if (!rec.verified_in_doc) {
            const upper = pick.value.toUpperCase();
            if ((sharedMpnSet && sharedMpnSet.has(upper)) || (rowMpnSet && rowMpnSet.has(upper))) {
              rec.verified_in_doc = true;
            }
          }
          if (rec._warn_invalid_code && rec.verified_in_doc) {
            delete rec._warn_invalid_code;
          }
        }
      }

      if (!rec.verified_in_doc) {
        const pnCandidate = String(rec.pn || rec.code || '').trim();
        if (pnCandidate) {
          const pnUpper = pnCandidate.toUpperCase();
          if (orderingPayload?.codes?.length) {
            const orderingCodes = new Set();
            for (const entry of orderingPayload.codes) {
              if (!entry) continue;
              const normalized = typeof entry === 'string'
                ? entry.trim().toUpperCase()
                : typeof entry === 'object' && entry.code != null
                  ? String(entry.code).trim().toUpperCase()
                  : null;
              if (normalized) orderingCodes.add(normalized);
            }
                        if (!orderingCodes.size && Array.isArray(orderingPayload.scored)) {
              for (const scored of orderingPayload.scored) {
                if (!scored || typeof scored !== 'object') continue;
                const normalized = typeof scored.code === 'string'
                  ? scored.code.trim().toUpperCase()
                  : null;
                if (normalized) orderingCodes.add(normalized);
              }
            }
            if (orderingCodes.has(pnUpper)) {
              rec.verified_in_doc = true;
            }
          }
          if (
            !rec.verified_in_doc &&
            ((sharedMpnSet && sharedMpnSet.has(pnUpper)) || (rowMpnSet && rowMpnSet.has(pnUpper)))
          ) {
            rec.verified_in_doc = true;
          }
        }
      }

      const guard = shouldInsert(rec, { coreSpecKeys: guardKeys, candidateSpecKeys });
      if (!guard.ok) {
        const skip = { reason: guard.reason, detail: guard.detail || null };
        if (rec.last_error) skip.last_error = rec.last_error;
        result.skipped.push(skip);
        continue;
      }

      const pnValue = String(rec.pn || rec.code || '').trim();
      const docType = String(options?.docType || '').toLowerCase();
      const requiresVoltage = Array.isArray(options?.coreSpecKeys) &&
        options.coreSpecKeys.some((k) => /coil_voltage/.test(String(k).toLowerCase()));
      if (docType === 'ordering' && requiresVoltage && !/\d/.test(pnValue)) {
        if (physicalCols.has('last_error')) rec.last_error = 'incomplete_pn';
        result.skipped.push({ reason: 'invalid_code', detail: 'missing_voltage_token' });
        continue;
      }
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
