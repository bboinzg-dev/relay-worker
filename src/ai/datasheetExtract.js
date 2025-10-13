'use strict';

/**
 * PDF → { brand, rows: [{ code, verified_in_doc, ...values }] }
 * - 전 패밀리 공용. 블루프린트(allowedKeys)로 values 키 제한.
 * - DocAI(Form Parser) 우선(표/텍스트 확보) → 실패 시 pdf-parse 폴백.
 * - 표의 Part No/Type/Model/Ordering Code에서 실제 PN 우선 수집.
 * - Ordering 정보는 조합 폭 과다 방지. 자유텍스트는 보조(regex).
 * - Gemini 2.5 Flash로 블루프린트 키에 맞춰 values만 채움(엄격 JSON).
 */

const { Storage } = require('@google-cloud/storage');
const storage = new Storage();

let DocumentProcessorServiceClient, VertexAI, pdfParse;
try { DocumentProcessorServiceClient = require('@google-cloud/documentai').v1.DocumentProcessorServiceClient; } catch {}
try { VertexAI = require('@google-cloud/vertexai').VertexAI; } catch {}
try { pdfParse = require('pdf-parse'); } catch {}

const db = require('../../db');
const { parseGcsUri } = require('../utils/gcs');
const { safeJsonParse } = require('../utils/safe-json');
const { explodeToRows } = require('../utils/mpn-exploder');
const { extractOrderingRecipe } = require('../utils/vertex');
const { extractOrderingInfo } = require('../utils/ordering-sections');
const { aiCanonicalizeKeys } = require('../pipeline/ai/canonKeys');

const MAX_PARTS = Number(process.env.MAX_ENUM_PARTS || 200);
const AUTO_ADD_FIELDS = /^(1|true|on)$/i.test(String(process.env.AUTO_ADD_FIELDS || ''));

/* -------------------- ENV helpers -------------------- */
function resolveDocAI() {
  const projectId =
    process.env.DOCAI_PROJECT_ID ||
    process.env.DOC_AI_PROJECT_ID ||
    process.env.GCP_PROJECT_ID ||
    process.env.GOOGLE_CLOUD_PROJECT;

  const location =
    process.env.DOCAI_LOCATION ||
    process.env.DOC_AI_LOCATION ||
    'us';

  const processorId =
    process.env.DOCAI_PROCESSOR_ID ||
    process.env.DOC_AI_PROCESSOR_ID;

  return { projectId, location, processorId };
}

function resolveGemini() {
  const project =
    process.env.GOOGLE_CLOUD_PROJECT ||
    process.env.GCP_PROJECT_ID;
  const location = process.env.VERTEX_LOCATION || 'asia-northeast3';
  const model = process.env.GEMINI_MODEL_EXTRACT || process.env.VERTEX_MODEL_ID || 'gemini-2.5-flash';
  return { project, location, model };
}

/* -------------------- brand detection -------------------- */
async function detectBrandFromText(fullText) {
  const r = await db.query(`SELECT brand, alias FROM public.manufacturer_alias`);
  const hay = (fullText || '').toLowerCase();
  let best = null;
  for (const row of r.rows) {
    const alias = String(row.alias || '').toLowerCase();
    const brand = String(row.brand || '').toLowerCase();
    if (!alias && !brand) continue;
    if ((alias && hay.includes(alias)) || (brand && hay.includes(brand))) {
      const cand = row.brand;
      if (!best || (alias && alias.length > best.len)) best = { brand: cand, len: alias.length || 0 };
    }
  }
  return best?.brand || null;
}

/* -------------------- DocAI (imagelessMode & 샘플링 폴백) -------------------- */
function samplePages(total, k) {
  if (!Number.isFinite(total) || total <= 0) return Array.from({ length: k }, (_,i)=>i+1);
  if (total <= k) return Array.from({ length: total }, (_,i)=>i+1);
  const pick = new Set();
  const first = Math.min(5, Math.floor(k/3));
  const last  = Math.min(5, Math.floor(k/3));
  for (let i=1; i<=first; i++) pick.add(i);
  for (let i=total-last+1; i<=total; i++) pick.add(i);
  while (pick.size < k) {
    const t = pick.size + 1;
    const pos = Math.max(1, Math.min(total, Math.round((t/(k+1))*total)));
    pick.add(pos);
  }
  return Array.from(pick).sort((a,b)=>a-b).slice(0,k);
}

async function processWithDocAI(gcsUri) {
  const { projectId, location, processorId } = resolveDocAI();
  if (!DocumentProcessorServiceClient || !projectId || !processorId) return null;

  const apiEndpoint = `${location}-documentai.googleapis.com`;
  const client = new DocumentProcessorServiceClient({ apiEndpoint });
  const name = `projects/${projectId}/locations/${location}/processors/${processorId}`;

  const { bucket, name: obj } = parseGcsUri(gcsUri);
  const [buf] = await storage.bucket(bucket).file(obj).download();

  let res;
  try {
    // 1차: imagelessMode (최대 30p)
    [res] = await client.processDocument({
      name,
      rawDocument: { content: buf, mimeType: 'application/pdf' },
      imagelessMode: true,
      skipHumanReview: true,
    });
  } catch (e) {
    const msg = String(e?.message || e);
    if (/supports up to 15 pages/i.test(msg) || /exceed the limit: 15/i.test(msg)) {
      let total = 0;
      try { if (pdfParse) { const parsed = await pdfParse(buf); total = Number(parsed?.numpages || 0); } } catch {}
      const pages = samplePages(total || 30, 15);
      const request = {
        name,
        rawDocument: { content: buf, mimeType: 'application/pdf' },
        processOptions: { individualPageSelector: { pages } },
        skipHumanReview: true,
      };
      [res] = await client.processDocument(request);
    } else {
      throw e;
    }
  }

  const doc = res?.document;
  if (!doc) return null;
  const fullText = doc.text || '';
  const pages = doc.pages || [];

  const getTxt = (layout) => {
    const segs = layout?.textAnchor?.textSegments || [];
    let out = '';
    for (const s of segs) out += fullText.slice(Number(s.startIndex || 0), Number(s.endIndex || 0));
    return out.trim();
  };

  const tables = [];
  for (const p of pages) {
    for (const t of (p.tables || [])) {
      const headRow = t.headerRows?.[0];
      const headers = (headRow?.cells || []).map(c => getTxt(c.layout));
      const rows = [];
      for (const br of (t.bodyRows || [])) rows.push((br.cells || []).map(c => getTxt(c.layout)));
      tables.push({ headers, rows });
    }
  }
  return { tables, fullText };
}

/* -------------------- pdf-parse fallback -------------------- */
async function parseTextWithPdfParse(gcsUri) {
  if (!pdfParse) return '';
  const { bucket, name } = parseGcsUri(gcsUri);
  const [buf] = await storage.bucket(bucket).file(name).download();
  const out = await pdfParse(buf);
  return out?.text || '';
}

/* -------------------- code extraction -------------------- */
function looksLikeCode(x) {
  const s = String(x || '').trim();
  if (!/^[A-Za-z0-9][A-Za-z0-9._/#-]{2,}$/.test(s)) return false;
  if (!/\d/.test(s)) return false;
  if (!/[A-Za-z]/.test(s) && !/[-_/]/.test(s)) return false;
  if (/\b(OHM|Ω|VDC|VAC|AMP|A|V|W|HZ|MS|SEC|UL|ROHS|REACH|DATE|PAGE)\b/i.test(s)) return false;
  return true;
}
function mapHeaderToKey(h) {
  const s = String(h || '').trim().toLowerCase().replace(/\s+/g, ' ');
  if (/\b(part( no\.?| number)|model|type|pn|ordering( code| number)?|catalog( no\.?| number)|item code|product code)\b/.test(s)) return 'code';
  if (/(부품|제품|제품형명|주문|형명|형번|품번|品番|型番|型号|型號)/.test(s)) return 'code';
  return null;
}
function extractCodesFromCell(cell) {
  const chunks = String(cell || '')
    .split(/[\s,;\/|]+/)
    .map((v) => v.trim())
    .filter(Boolean);
  const out = [];
  for (const chunk of chunks) {
    const normalized = chunk.toUpperCase();
    if (!looksLikeCode(normalized)) continue;
    out.push(normalized);
  }
  return out;
}
function codesFromDocAiTables(tables) {
  const set = new Set();
  for (const t of (tables || [])) {
    const headers = Array.isArray(t.headers) ? t.headers : [];
    const rows = Array.isArray(t.rows) ? t.rows : [];
    const width = Math.max(headers.length, ...rows.map((r) => (Array.isArray(r) ? r.length : 0)));
    if (!width) continue;

    const stats = Array.from({ length: width }, () => ({
      cells: 0,
      rowsWithCodes: 0,
      distinct: new Set(),
    }));

    const headerCodeCols = new Set();
    headers.forEach((h, i) => {
      const key = mapHeaderToKey(h);
      if (key === 'code') headerCodeCols.add(i);
    });

    rows.forEach((row) => {
      if (!Array.isArray(row)) return;
      for (let i = 0; i < width; i += 1) {
        const cell = row[i];
        if (cell == null) continue;
        const text = String(cell).trim();
        if (!text) continue;
        const codes = extractCodesFromCell(text);
        if (!codes.length) {
          stats[i].cells += 1;
          continue;
        }
        stats[i].cells += 1;
        stats[i].rowsWithCodes += 1;
        for (const code of codes) stats[i].distinct.add(code);
      }
    });

    const heuristicCols = new Set();
    stats.forEach((info, i) => {
      const distinctCount = info.distinct.size;
      if (!distinctCount) return;
      const ratio = info.rowsWithCodes / Math.max(info.cells, 1);
      if (ratio >= 0.4 || distinctCount >= 3 || headerCodeCols.has(i)) {
        heuristicCols.add(i);
      }
    });

    if (!heuristicCols.size && headerCodeCols.size) {
      headerCodeCols.forEach((i) => heuristicCols.add(i));
    }
    if (!heuristicCols.size) continue;

    rows.forEach((row) => {
      if (!Array.isArray(row)) return;
      heuristicCols.forEach((colIdx) => {
        const cell = row[colIdx];
        if (cell == null) return;
        const text = String(cell).trim();
        if (!text) return;
        for (const code of extractCodesFromCell(text)) set.add(code);
      });
    });
  }
  return Array.from(set);
}

function escapeRegex(value) {
  return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function escapeRegexWithWildcards(value) {
  const str = String(value || '').toUpperCase();
  let out = '';
  for (const ch of str) {
    if (ch === '*') {
      out += '[A-Z0-9]*';
      continue;
    }
    if (ch === '?') {
      out += '[A-Z0-9]';
      continue;
    }
    if (ch === '#') {
      out += '\\d';
      continue;
    }
    out += escapeRegex(ch);
  }
  return out;
}

function applyTemplateOpsForRegex(value, options = []) {
  const arr = Array.isArray(options) ? options : [];
  const first = Array.isArray(value) ? value[0] : value;
  let current = first == null ? '' : String(first);
  for (const rawOption of arr) {
    if (!rawOption) continue;
    const token = String(rawOption).trim();
    if (!token) continue;
    const normalized = token.includes('=') ? token.replace('=', ':') : token;
    const lower = normalized.toLowerCase();
    if (lower === 'upper' || lower === 'uppercase' || lower === 'upcase') {
      current = current.toUpperCase();
      continue;
    }
    if (lower === 'lower' || lower === 'downcase' || lower === 'lowercase') {
      current = current.toLowerCase();
      continue;
    }
    if (lower === 'first') {
      current = current.split(',')[0].trim();
      continue;
    }
    if (lower === 'alnum') {
      current = current.replace(/[^0-9A-Z]/gi, '');
      continue;
    }
    if (lower === 'digits') {
      current = current.replace(/[^0-9]/g, '');
      continue;
    }
    if (lower === 'num') {
      const match = current.match(/-?\d+(?:\.\d+)?/);
      current = match ? match[0] : '';
      continue;
    }
    if (lower.startsWith('pad:')) {
      const parts = normalized.split(':');
      const width = Number(parts[1]) || 0;
      const fillRaw = parts.length > 2 ? parts[2] : '';
      const fill = fillRaw && fillRaw.trim() ? fillRaw.trim()[0] : '0';
      if (width > 0) current = current.padStart(width, fill);
      continue;
    }
    if (lower.startsWith('slice:')) {
      const parts = normalized.split(':');
      const start = Number(parts[1]) || 0;
      const end = parts.length > 2 && parts[2] !== '' ? Number(parts[2]) : undefined;
      current = current.slice(start, Number.isNaN(end) ? undefined : end);
      continue;
    }
    if (lower.startsWith('map:')) {
      const entries = normalized.slice(4).split(',');
      const mapping = Object.create(null);
      for (const entry of entries) {
        const [from, to] = entry.split('>');
        if (!from || to == null) continue;
        mapping[String(from).trim().toUpperCase()] = String(to).trim();
      }
      const key = String(current).trim().toUpperCase();
      if (Object.prototype.hasOwnProperty.call(mapping, key)) {
        current = mapping[key];
      }
      continue;
    }
    if (lower.startsWith('prefix:')) {
      const [, rawPrefix = ''] = normalized.split(':');
      current = `${rawPrefix}${current}`;
      continue;
    }
    if (lower.startsWith('suffix:')) {
      const [, rawSuffix = ''] = normalized.split(':');
      current = `${current}${rawSuffix}`;
      continue;
    }
    if (lower.startsWith('replace:')) {
      const [, rawArgs = ''] = normalized.split(':');
      const [search, replacement = ''] = rawArgs.split('>');
      if (search != null) {
        const matcher = new RegExp(escapeRegex(search), 'g');
        current = current.replace(matcher, replacement);
      }
      continue;
    }
  }
  return current.trim();
}

function parseMapOutputs(options = []) {
  const outputs = [];
  for (const rawOption of options) {
    if (!rawOption) continue;
    const token = String(rawOption).trim();
    if (!token) continue;
    const normalized = token.includes('=') ? token.replace('=', ':') : token;
    const lower = normalized.toLowerCase();
    if (!lower.startsWith('map:')) continue;
    const entries = normalized.slice(4).split(',');
    for (const entry of entries) {
      const [, to] = entry.split('>');
      if (to == null) continue;
      const clean = String(to).trim();
      if (clean) outputs.push(clean);
    }
  }
  return outputs;
}

function findVariantDomainValues(key, variantDomains = {}) {
  const raw = String(key || '').trim();
  if (!raw) return [];
  const lower = raw.toLowerCase();
  const normalized = lower.replace(/[^a-z0-9]/g, '');
  const results = [];
  for (const [domainKey, domainValues] of Object.entries(variantDomains || {})) {
    const domainStr = String(domainKey || '').trim();
    if (!domainStr) continue;
    const domainLower = domainStr.toLowerCase();
    const domainNormalized = domainLower.replace(/[^a-z0-9]/g, '');
    if (domainStr === raw || domainLower === lower || domainNormalized === normalized) {
      const list = Array.isArray(domainValues) ? domainValues : [domainValues];
      for (const value of list) {
        const str = value == null ? '' : String(value).trim();
        if (!str) continue;
        results.push(str);
      }
    }
  }
  return results;
}

function buildPlaceholderRegex(body, variantDomains) {
  const tokens = String(body || '')
    .split('|')
    .map((part) => part.trim())
    .filter(Boolean);
  if (!tokens.length) return '';
  const baseKey = tokens.shift();
  const options = tokens;
  const domainValues = findVariantDomainValues(baseKey, variantDomains);
  const processed = [];
  let hasEmpty = false;
  if (domainValues.length) {
    for (const value of domainValues) {
      const applied = applyTemplateOpsForRegex(value, options);
      if (applied == null) continue;
      const trimmed = String(applied).trim();
      if (!trimmed) {
        hasEmpty = true;
        continue;
      }
      processed.push(trimmed.toUpperCase());
    }
  }
  if (!processed.length) {
    const mapOutputs = parseMapOutputs(options);
    if (mapOutputs.length) {
      for (const output of mapOutputs) {
        const trimmed = String(output).trim();
        if (!trimmed) {
          hasEmpty = true;
          continue;
        }
        processed.push(trimmed.toUpperCase());
      }
    }
  }
  const unique = Array.from(new Set(processed));
  if (!unique.length) {
    if (hasEmpty) return '';
    if (options.some((op) => String(op).toLowerCase().includes('digit'))) {
      const padOption = options.find((op) => /^pad[:=]/i.test(String(op)));
      if (padOption) {
        const normalizedPad = String(padOption).replace('=', ':');
        const [, widthRaw = ''] = normalizedPad.split(':');
        const width = Number(widthRaw) || 0;
        if (width > 0) return `\\d{${width}}`;
      }
      return '\\d+';
    }
    if (options.some((op) => String(op).toLowerCase().includes('alnum'))) {
      return '[A-Z0-9]+';
    }
    return '[A-Z0-9]+';
  }
  const patterns = unique.map((value) => escapeRegexWithWildcards(value)).filter(Boolean);
  if (!patterns.length) {
    if (hasEmpty) return '';
    return '[A-Z0-9]+';
  }
  let combined;
  if (patterns.length === 1) combined = patterns[0];
  else combined = `(?:${patterns.join('|')})`;
  if (hasEmpty) combined = `(?:${combined})?`;
  return combined;
}

function buildPnRegexFromTemplate(template, variantDomains = {}) {
  const tpl = typeof template === 'string' ? template.trim() : '';
  if (!tpl) return null;
  const parts = [];
  const pattern = /\{\{?\s*([^{}]+?)\s*\}\}?/g;
  let lastIndex = 0;
  let match;
  while ((match = pattern.exec(tpl)) !== null) {
    if (match.index > lastIndex) {
      const literal = tpl.slice(lastIndex, match.index);
      if (literal) parts.push(escapeRegex(literal));
    }
    const placeholder = buildPlaceholderRegex(match[1], variantDomains);
    if (placeholder) parts.push(placeholder);
    lastIndex = match.index + match[0].length;
  }
  if (lastIndex < tpl.length) {
    const tail = tpl.slice(lastIndex);
    if (tail) parts.push(escapeRegex(tail));
  }
  if (!parts.length) return null;
  const combined = parts.join('');
  if (!combined) return null;
  try {
    return new RegExp(`^(?:${combined})$`, 'i');
  } catch (err) {
    console.warn('[pn-regex] failed to build regex from template:', err?.message || err);
    return null;
  }
}

function tokenizeForPn(code) {
  if (!code) return [];
  const tokens = [];
  const hay = String(code).trim();
  if (!hay) return tokens;
  const re = /(\d+[A-Z]?|[A-Z]+|[^A-Z0-9]+)/g;
  let match;
  while ((match = re.exec(hay)) !== null) {
    const token = match[0];
    if (!token) continue;
    tokens.push(token);
  }
  return tokens;
}

function classifyPnToken(token) {
  if (!token) return 'other';
  const str = String(token);
  if (/^\d+$/.test(str)) return 'digit';
  if (/^[A-Z]+$/.test(str)) return 'alpha';
  if (/^\d+[A-Z]+$/.test(str)) return 'alnum';
  return 'other';
}

function canAcceptPnColumn(column, tokenType) {
  if (!column || !tokenType) return false;
  if (!column.type) return true;
  if (column.type === tokenType) return true;
  if (column.type === 'digit' && tokenType === 'alnum') return true;
  if (column.type === 'alnum' && tokenType === 'digit') return true;
  return false;
}

function mergePnColumnType(current, tokenType) {
  if (!current) return tokenType;
  if (current === tokenType) return current;
  if ((current === 'digit' && tokenType === 'alnum') || (current === 'alnum' && tokenType === 'digit')) {
    return 'alnum';
  }
  return tokenType;
}

function buildPnRegexFromExamples(examples) {
  if (!Array.isArray(examples) || !examples.length) return null;
  const normalized = examples
    .map((code) => String(code || '').trim().toUpperCase())
    .filter((code) => code && /^[A-Z0-9][A-Z0-9._/#-]{1,}$/i.test(code));
  if (!normalized.length) return null;

  const columns = [];
  const total = normalized.length;

  for (const code of normalized.slice(0, MAX_PARTS)) {
    const tokens = tokenizeForPn(code);
    if (!tokens.length) continue;
    let colIdx = 0;
    for (const token of tokens) {
      const tokenType = classifyPnToken(token);
      let placed = false;
      while (colIdx < columns.length) {
        const col = columns[colIdx];
        if (canAcceptPnColumn(col, tokenType)) {
          col.tokens.add(token);
          col.count += 1;
          col.type = mergePnColumnType(col.type, tokenType);
          placed = true;
          colIdx += 1;
          break;
        }
        col.optional = true;
        colIdx += 1;
      }
      if (!placed) {
        columns.push({
          tokens: new Set([token]),
          type: tokenType,
          count: 1,
          optional: false,
        });
        colIdx = columns.length;
      }
    }
    while (colIdx < columns.length) {
      columns[colIdx].optional = true;
      colIdx += 1;
    }
  }

  if (!columns.length) return null;

  const parts = [];
  for (const col of columns) {
    const options = Array.from(col.tokens);
    if (!options.length) continue;
    if (options.length > 80) return null;
    const sorted = options.sort((a, b) => a.localeCompare(b));
    let part;
    if (sorted.length === 1) {
      part = escapeRegex(sorted[0]);
    } else {
      part = `(?:${sorted.map((opt) => escapeRegex(opt)).join('|')})`;
    }
    if (col.optional || col.count < total) {
      part = `(?:${part})?`;
    }
    parts.push(part);
  }

  if (!parts.length) return null;
  const pattern = `^(?:${parts.join('')})$`;
  try {
    return new RegExp(pattern);
  } catch (err) {
    console.warn('[pn-regex] failed to build regex:', err?.message || err);
    return null;
  }
}
function codesFromFreeText(txt) {
  const set = new Set();
  const re = /\b([A-Z0-9][A-Z0-9\-_/\.]{2,})\b/g;
  let m; while ((m = re.exec(txt)) !== null) {
    const cand = m[1].toUpperCase();
    if (looksLikeCode(cand)) set.add(cand);
    if (set.size > MAX_PARTS) break;
  }
  return Array.from(set);
}

function normalizeCodeKey(value) {
  return String(value || '').trim().toUpperCase();
}

function formatTableRow(row) {
  if (!Array.isArray(row)) return '';
  return row
    .map((cell) => (cell == null ? '' : String(cell).trim()))
    .join(' | ')
    .trim();
}

function gatherPerCodeTablePreview(tables, codes, options = {}) {
  const context = Number.isFinite(options.contextRows)
    ? Math.max(0, options.contextRows)
    : 2;
  const maxMatchesPerCode = Number.isFinite(options.maxMatchesPerCode)
    ? Math.max(1, options.maxMatchesPerCode)
    : 1;
  const normalizedCodes = Array.isArray(codes)
    ? codes
        .map((code) => normalizeCodeKey(code))
        .filter(Boolean)
    : [];
  if (!normalizedCodes.length || !Array.isArray(tables) || !tables.length) {
    return new Map();
  }

  const pending = new Map();
  for (const code of normalizedCodes) {
    if (!pending.has(code)) pending.set(code, 0);
  }

  const previews = new Map();

  for (const table of tables) {
    if (!pending.size) break;
    const headers = Array.isArray(table?.headers) ? table.headers : [];
    const headerLine = formatTableRow(headers);
    const rows = Array.isArray(table?.rows) ? table.rows : [];
    for (let i = 0; i < rows.length && pending.size; i += 1) {
      const row = rows[i];
      const rowText = normalizeCodeKey(Array.isArray(row) ? row.join(' ') : '');
      if (!rowText) continue;
      for (const code of pending.keys()) {
        if (!rowText.includes(code)) continue;
        const start = Math.max(0, i - context);
        const end = Math.min(rows.length - 1, i + context);
        const snippetRows = [];
        for (let idx = start; idx <= end; idx += 1) {
          const formatted = formatTableRow(rows[idx]);
          if (formatted) snippetRows.push(formatted);
        }
        const snippet = [
          headerLine ? `HEADER: ${headerLine}` : null,
          snippetRows.join('\n'),
        ]
          .filter(Boolean)
          .join('\n');
        if (!previews.has(code)) previews.set(code, snippet);
        const hits = pending.get(code) || 0;
        if (hits + 1 >= maxMatchesPerCode) {
          pending.delete(code);
        } else {
          pending.set(code, hits + 1);
        }
      }
    }
  }

  return previews;
}

function extractTypePartPairs(tables = []) {
  const pairs = new Map();
  for (const table of tables) {
    const headers = Array.isArray(table?.headers)
      ? table.headers.map((h) => String(h || '').toLowerCase())
      : [];
    const typeIdx = headers.findIndex((h) => /(^|\s)type(\s|$)|type no/.test(h));
    const partIdx = headers.findIndex((h) => /(^|\s)part(\s|$)|part no|品番|型番/.test(h));
    if (typeIdx < 0 || partIdx < 0) continue;
    for (const row of table?.rows || []) {
      if (!Array.isArray(row)) continue;
      const type = String(row[typeIdx] || '').trim().toUpperCase();
      const part = String(row[partIdx] || '').trim().toUpperCase();
      if (!type || !part) continue;
      if (!/[0-9]/.test(type) || !/[0-9]/.test(part)) continue;
      if (!pairs.has(type)) pairs.set(type, new Set());
      pairs.get(type).add(part);
    }
  }
  const out = new Map();
  for (const [key, valueSet] of pairs) out.set(key, Array.from(valueSet));
  return out;
}

function gatherOrderingSectionEvidence(orderingInfo, code, options = {}) {
  const rawCode = String(code || '').trim();
  if (!rawCode) return null;
  const sectionText = orderingInfo?.text;
  if (!sectionText) return null;

  const context = Number.isFinite(options.contextLines)
    ? Math.max(0, options.contextLines)
    : 2;
  const tokens = rawCode
    .toUpperCase()
    .split(/[^A-Z0-9]+/g)
    .map((token) => token.trim())
    .filter((token) => token.length >= 2);
  if (!tokens.length) return null;

  const lines = sectionText
    .split(/\r?\n/g)
    .map((line) => String(line || '').trim())
    .filter(Boolean);
  if (!lines.length) return null;

  let bestSnippet = null;
  let bestScore = 0;

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    if (!line) continue;
    const upperLine = line.toUpperCase();
    let matches = 0;
    for (const token of tokens) {
      if (upperLine.includes(token)) matches += 1;
    }
    if (!matches) continue;

    const start = Math.max(0, i - context);
    const end = Math.min(lines.length - 1, i + context);
    const snippetLines = lines.slice(start, end + 1).filter(Boolean);
    if (!snippetLines.length) continue;
    const snippet = snippetLines.join('\n');
    if (matches > bestScore || (matches === bestScore && snippet.length < (bestSnippet?.length || Infinity))) {
      bestSnippet = snippet;
      bestScore = matches;
    }
  }

  if (!bestSnippet && lines.length) {
    const limited = lines.slice(0, Math.min(lines.length, Math.max(3, context * 2 + 1)));
    const header = `TOKENS: ${tokens.join(' / ')}`;
    bestSnippet = [header, ...limited].join('\n');
  }

  if (!bestSnippet) return null;
  return `ORDERING SECTION:\n${bestSnippet}`;
}

/* -------------------- Gemini mapping -------------------- */
async function geminiMapValues({ family, brandHint, codes, allowedKeys, docText, tablePreview }) {
  const { project, location, model } = resolveGemini();
  if (!VertexAI || !project) return {};

  const vertex = new VertexAI({ project, location });
  const mdl = vertex.getGenerativeModel({
    model,
    systemInstruction: { parts: [{ text: [
      `You extract component specs from datasheets.`,
      `Category (family): "${family}".`,
      `Return ONLY strict JSON. Schema: {"parts":[{"brand":"string","code":"string","values":{...}}]}.`,
      `Allowed "values" keys: ${allowedKeys.join(', ') || '(none)'}. Do not invent keys.`,
      `Only include codes from the provided list (do not fabricate).`,
      `Normalize numbers to plain numbers (no units). Omit missing.`
    ].join('\n') }] }
  });

  const userText = [
    brandHint ? `brand_hint: ${brandHint}` : '',
    `codes: ${codes.join(', ')}`,
    tablePreview ? `TABLES:\n${tablePreview}` : '',
    `TEXT:\n${(docText || '').slice(0, 200000)}`
  ].filter(Boolean).join('\n\n');

  const resp = await mdl.generateContent({
    contents: [{ role: 'user', parts: [{ text: userText }]}],
    generationConfig: {
      temperature: 0.2,
      topP: 0.8,
      responseMimeType: 'application/json',
      maxOutputTokens: 8192,
    },
  });

  const txt = resp?.response?.candidates?.[0]?.content?.parts?.[0]?.text || '{}';
  let parsed = {};
  try {
    parsed = safeJsonParse(txt) || {};
  } catch {
    parsed = {};
  }
 
   const parts = Array.isArray(parsed?.parts) ? parsed.parts : [];
   const tableHint = typeof parsed?.table_hint === 'string' ? parsed.table_hint : null;
   const pnTemplate = typeof parsed?.pn_template === 'string' && parsed.pn_template.trim()
     ? parsed.pn_template.trim()
     : null;
   const variantDomains = parsed?.variant_domains && typeof parsed.variant_domains === 'object'
     ? parsed.variant_domains
     : null;
 
   return { parts, table_hint: tableHint, pn_template: pnTemplate, variant_domains: variantDomains };
 }
 
 function normalizeVariantDomainMap(raw) {
   const normalized = {};
   if (!raw || typeof raw !== 'object') return normalized;
   for (const [rawKey, rawValue] of Object.entries(raw)) {
     const key = String(rawKey || '').trim();
     if (!key) continue;
     const values = Array.isArray(rawValue) ? rawValue : [rawValue];
     const seen = new Set();
     const list = [];
     for (const candidate of values) {
       if (candidate == null) continue;
       const str = String(candidate).trim();
       const marker = str.toLowerCase();
       if (seen.has(marker)) continue;
       seen.add(marker);
       list.push(str);
     }
     if (list.length) normalized[key] = list;
   }
   return normalized;
 }
 
 function mergeVariantDomainMaps(primary, secondary) {
   const out = { ...primary };
   for (const [key, values] of Object.entries(secondary || {})) {
     if (!out[key]) {
       out[key] = [...values];
       continue;
     }
     const seen = new Set(out[key].map((v) => v.toLowerCase()));
     for (const value of values) {
       const norm = value.toLowerCase();
       if (seen.has(norm)) continue;
       seen.add(norm);
       out[key].push(value);
     }
   }
   return out; 
}

/* -------------------- Public: main extractor -------------------- */
async function extractPartsAndSpecsFromPdf({ gcsUri, allowedKeys, family = null, brandHint = null }) {
  const rowAllowedKeySet = new Set();
  const rowAllowedKeys = [];
  const ensureAllowedKey = (value) => {
    const str = value == null ? '' : String(value).trim();
    if (!str) return;
    const lower = str.toLowerCase();
    if (rowAllowedKeySet.has(lower)) return;
    rowAllowedKeySet.add(lower);
    rowAllowedKeys.push(str);
  };

  if (Array.isArray(allowedKeys)) {
    for (const key of allowedKeys) ensureAllowedKey(key);
  }
  ensureAllowedKey('series');
  ensureAllowedKey('series_code');
  ensureAllowedKey('pn_jp');
  ensureAllowedKey('pn_aliases');
  ensureAllowedKey('ordering_market');

  const familyLower = String(family || '').trim().toLowerCase();

  if (familyLower.includes('capacitor')) {
    [
      'capacitance_uf',
      'rated_voltage_v',
      'esr_mohm',
      'df_percent',
      'dcl_ua',
      'case_size_code',
      'product_category',
      'msl',
      'rms_current_100khz_45c',
      'rms_current_100khz_85c',
      'rms_current_100khz_105c',
      'rms_current_100khz_125c',
    ].forEach(ensureAllowedKey);
  }

    if (familyLower.startsWith('relay')) {
    [
      'contact_rating_text',
      'dielectric_strength_v',
      'operate_time_ms',
      'release_time_ms',
      'coil_resistance_ohm',
      'insulation_resistance_mohm',
      'length_mm',
      'width_mm',
      'height_mm',
      'mount_type',
      'packing_style',
    ].forEach(ensureAllowedKey);
  }

  const promptAllowedKeys = Array.from(new Set([
    ...rowAllowedKeys,
  ]));

  let docai = await processWithDocAI(gcsUri);
  let fullText = docai?.fullText || '';
  if (!fullText) fullText = await parseTextWithPdfParse(gcsUri);

  const tableList = Array.isArray(docai?.tables) ? docai.tables : [];
  const typePartMap = extractTypePartPairs(tableList);

  const brand = brandHint || (await detectBrandFromText(fullText)) || 'unknown';
  const orderingInfo = extractOrderingInfo(fullText, MAX_PARTS);

  // 코드 후보
  let codes = [];
  if (tableList.length) codes = codesFromDocAiTables(tableList);
  if (!codes.length && fullText) codes = codesFromFreeText(fullText);
  let pnRegex = buildPnRegexFromExamples(codes.slice(0, MAX_PARTS));

  // 표 프리뷰(LLM 컨텍스트)
  let tablePreview = '';
  let perCodePreviewMap = new Map();
  const baseSegments = [];
  if (tableList.length) {
    baseSegments.push(
      ...tableList.slice(0, 6).map((t) => {
        const head = (t.headers || []).join(' | ');
        const rows = (t.rows || []).slice(0, 40).map((r) => r.join(' | ')).join('\n');
        return `HEADER: ${head}\n${rows}`;
      })
    );
    perCodePreviewMap = gatherPerCodeTablePreview(tableList, codes.slice(0, MAX_PARTS), {
      contextRows: 2,
      maxMatchesPerCode: 2,
    });
  }
  const perCodeSegments = [];
  const seenPreviewCodes = new Set();
  let appended = 0;
  const previewOrder = codes.slice(0, MAX_PARTS);
  for (const code of previewOrder) {
    const norm = normalizeCodeKey(code);
    if (!norm || seenPreviewCodes.has(norm)) continue;
    let snippet = perCodePreviewMap.get(norm);
    if (!snippet) {
      const orderingSnippet = gatherOrderingSectionEvidence(orderingInfo, code, { contextLines: 2 });
      if (orderingSnippet) {
        if (!(perCodePreviewMap instanceof Map)) perCodePreviewMap = new Map();
        perCodePreviewMap.set(norm, orderingSnippet);
        snippet = orderingSnippet;
      }
    }
    if (!snippet) continue;
    seenPreviewCodes.add(norm);
    perCodeSegments.push(`CODE: ${code}\n${snippet}`);
    appended += 1;
    if (appended >= 50) break;
  }
  const combinedSegments = [];
  if (baseSegments.length) combinedSegments.push(...baseSegments);
  if (perCodeSegments.length) combinedSegments.push(...perCodeSegments);
  if (combinedSegments.length) {
    tablePreview = combinedSegments.join('\n---\n');
  }

  // Gemini로 values 매핑(블루프린트 키만)
  const mappedResult = await geminiMapValues({
    family,
    brandHint: brand,
    codes: codes.slice(0, MAX_PARTS),
    allowedKeys: promptAllowedKeys,
    docText: fullText,
    tablePreview
  });

  const mappedParts = Array.isArray(mappedResult?.parts) ? mappedResult.parts : [];
  const tableHintRaw = String(mappedResult?.table_hint || '');
  const orderingHint = tableHintRaw.toUpperCase().includes('ORDERING');

  const canonicalKeyMap = new Map();
  if (mappedParts.length) {
    const candidateKeys = new Set();
    for (const part of mappedParts) {
      const partValues = part?.values && typeof part.values === 'object' ? part.values : {};
      for (const rawKey of Object.keys(partValues || {})) {
        const keyStr = rawKey == null ? '' : String(rawKey).trim();
        if (!keyStr) continue;
        const lower = keyStr.toLowerCase();
        if (!rowAllowedKeySet.has(lower)) candidateKeys.add(keyStr);
      }
    }
    const candidateList = Array.from(candidateKeys).slice(0, 120);
    for (const key of candidateList) canonicalKeyMap.set(key, key);
    if (candidateList.length) {
      try {
        const knownList = Array.from(rowAllowedKeys);
        const { map } = await aiCanonicalizeKeys(
          family || 'generic',
          candidateList,
          knownList,
        );
        if (map && typeof map === 'object') {
          for (const key of candidateList) {
            const rec = map[key] || {};
            const action = String(rec.action || '').toLowerCase();
            let canonical = String(rec.canonical || key).trim();
            if (action !== 'map') canonical = key;
            if (!canonical) canonical = key;
            canonicalKeyMap.set(key, canonical);
            if (!canonicalKeyMap.has(canonical)) canonicalKeyMap.set(canonical, canonical);
            ensureAllowedKey(canonical);
          }
        } else {
          candidateList.forEach((key) => ensureAllowedKey(key));
        }
      } catch (err) {
        console.warn('[datasheet] aiCanonicalizeKeys failed:', err?.message || err);
        candidateList.forEach((key) => ensureAllowedKey(key));
      }
    }
  }

  const out = [];
  const seenRows = new Set();
  const mergedCodes = [];
  const seenCodes = new Set();
  const seedCodes = codes.slice(0, MAX_PARTS);

  const perCodeEvidence = new Set(perCodePreviewMap instanceof Map ? perCodePreviewMap.keys() : []);
  const docTextHaystack = typeof fullText === 'string' ? fullText.toUpperCase() : '';
  const hasDocEvidence = (norm) => {
    if (!norm) return false;
    if (perCodeEvidence.has(norm)) return true;
    if (docTextHaystack && docTextHaystack.includes(norm)) return true;
    return false;
  };

  const hasOrderingEvidence = (code) => {
    // 주문 섹션에서 코드 토큰들이 같은 라인/인접 라인에 함께 나오면 true
    if (!code) return false;
    const snip = gatherOrderingSectionEvidence(orderingInfo, code, { contextLines: 2 });
    if (!snip || snip.length < 20) return false;
    const tokens = String(code)
      .toUpperCase()
      .split(/[^A-Z0-9]+/)
      .map((part) => part.trim())
      .filter((part) => part && (part.length >= 2 || /\d/.test(part)));
    if (!tokens.length) return false;
    const lines = snip
      .split(/\n+/)
      .map((line) => line.trim().toUpperCase())
      .filter(Boolean);
    for (let i = 0; i < lines.length; i += 1) {
      const windowText = [lines[i], lines[i + 1] || ''].filter(Boolean).join(' ');
      if (!windowText) continue;
      if (tokens.every((token) => windowText.includes(token))) {
        return true;
      }
    }
    return false;
  };

  const hasDocEvidenceValue = (value) => hasDocEvidence(normalizeCodeKey(value));

  const pushCode = (value) => {
    const norm = String(value || '').trim().toUpperCase();
    if (!norm || seenCodes.has(norm)) return;
    seenCodes.add(norm);
    mergedCodes.push(norm);
  };

  seedCodes.forEach(pushCode);

  const pushRow = ({ code, values = {}, brand: rowBrand, verified }) => {
    const norm = String(code || '').trim().toUpperCase();
    if (!norm || seenRows.has(norm)) return;
    seenRows.add(norm);
    const row = { code: norm, verified_in_doc: Boolean(verified || hasDocEvidence(norm)) };
    const brandValue = rowBrand || brand;
    if (brandValue) row.brand = brandValue;
    if (values && typeof values === 'object') {
      const normalizedValues = {};
      const assignValue = (key, rawValue) => {
        const keyStr = key == null ? '' : String(key).trim();
        if (!keyStr) return;
        const canonicalKey = canonicalKeyMap.has(keyStr) ? canonicalKeyMap.get(keyStr) : keyStr;
        const cleanKey = canonicalKey == null ? '' : String(canonicalKey).trim();
        if (!cleanKey) return;
        const lower = cleanKey.toLowerCase();
        if (!rowAllowedKeySet.has(lower)) {
          if (!AUTO_ADD_FIELDS) return;
          ensureAllowedKey(cleanKey);
        }
        normalizedValues[cleanKey] = rawValue;
      };

      for (const [rawKey, rawValue] of Object.entries(values)) {
        if (rawValue == null) continue;
        assignValue(rawKey, rawValue);
      }

      if (typePartMap.has(norm)) {
        const jpList = typePartMap.get(norm);
        assignValue('pn_jp', Array.isArray(jpList) && jpList.length ? jpList[0] : null);
        assignValue('pn_aliases', jpList && jpList.length ? jpList : null);
        assignValue('ordering_market', 'GLOBAL');
      } else if (!/^[A-Z]HE[0-9]/.test(norm) && /^AHE[0-9]/.test(norm)) {
        assignValue('ordering_market', 'JP');
      }

      const allowedSnapshot = Array.from(rowAllowedKeys);
      for (const key of allowedSnapshot) {
        if (!key) continue;
        if (Object.prototype.hasOwnProperty.call(normalizedValues, key) && normalizedValues[key] != null) {
          row[key] = normalizedValues[key];
        }
      }
    }
    out.push(row);
    pushCode(norm);
  };

  for (const part of mappedParts) {
    const partValues = part?.values && typeof part.values === 'object' ? part.values : {};
    pushRow({ code: part?.code, values: partValues, brand: part?.brand, verified: true });
  }

  let orderingExpanded = false;
  let orderingDomains = normalizeVariantDomainMap(mappedResult?.variant_domains);
  let pnTemplate = typeof mappedResult?.pn_template === 'string' && mappedResult.pn_template.trim()
    ? mappedResult.pn_template.trim()
    : null;
  const recipeInput = gcsUri || orderingInfo?.text || tablePreview || (fullText ? String(fullText).slice(0, 6000) : '');
  try {
    const recipe = await extractOrderingRecipe(recipeInput);
    orderingDomains = mergeVariantDomainMaps(
      orderingDomains,
      normalizeVariantDomainMap(recipe?.variant_domains),
    );
    if (!pnTemplate && typeof recipe?.pn_template === 'string' && recipe.pn_template.trim()) {
      pnTemplate = recipe.pn_template.trim();
    }
  } catch (err) {
    console.warn('[ordering] extractOrderingRecipe failed:', err?.message || err);
  }

  if (!pnRegex && pnTemplate) {
    const templateRegex = buildPnRegexFromTemplate(pnTemplate, orderingDomains);
    if (templateRegex) pnRegex = templateRegex;
  }

  const variantKeys = Object.keys(orderingDomains)
    .map((key) => String(key || '').trim())
    .filter(Boolean);
  if (variantKeys.length) {
    for (const key of variantKeys) ensureAllowedKey(key);
    const baseSeries = orderingDomains.series_code?.[0] || orderingDomains.series?.[0] || null;
    const orderingBase = {
      brand,
      series: baseSeries,
      series_code: baseSeries,
      values: orderingDomains,
    };
    const generatedRows = explodeToRows(orderingBase, { variantKeys, pnTemplate }) || [];
    const beforeCount = out.length;
    for (const generated of generatedRows) {
      if (!generated || typeof generated !== 'object') continue;
      const rawCode = generated.code;
      const codeStr = String(rawCode ?? '').trim();
      if (!codeStr) continue;
      // 표 예시/템플릿으로 만든 pnRegex가 있으면 반드시 가드
      if (pnRegex && !pnRegex.test(codeStr)) continue;
      const values = generated.values && typeof generated.values === 'object' ? generated.values : {};
      const v = hasDocEvidence(normalizeCodeKey(codeStr)) || hasOrderingEvidence(codeStr);
      if (values && typeof values === 'object' && !Object.prototype.hasOwnProperty.call(values, '_pn_template')) {
        values._pn_template = pnTemplate || null;
      }
      pushRow({ code: codeStr, values, brand, verified: v });
    }
    orderingExpanded = out.length > beforeCount;
  }

  if (!out.length) {
    for (const c of seedCodes) {
      pushRow({ code: c, values: {}, brand, verified: hasDocEvidenceValue(c) });
    }
  }

  const codeList = mergedCodes.slice(0, MAX_PARTS);
  const uniqueRowCodes = new Set();
  for (const row of out) {
    if (!row || typeof row !== 'object') continue;
    const code = String(row.code || row.pn || '').trim().toUpperCase();
    if (code) uniqueRowCodes.add(code);
  }
  const uniqueCandidateCodes = new Set(codeList.map((c) => String(c || '').trim().toUpperCase()).filter(Boolean));
  let docType = 'single';
  if (orderingInfo || orderingExpanded || orderingHint) docType = 'ordering';
  else if (uniqueRowCodes.size > 1 || uniqueCandidateCodes.size > 1) docType = 'catalog';

  return {
    brand,
    rows: out.slice(0, MAX_PARTS),
    text: fullText,
    tables: tableList,
    codes: codeList,
    mpn_list: codeList,
    ordering_info: orderingInfo || null,
    doc_type: docType,
  };
}

module.exports = { extractPartsAndSpecsFromPdf };
