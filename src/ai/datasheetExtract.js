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

const MAX_PARTS = Number(process.env.MAX_ENUM_PARTS || 200);

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
  let rowAllowedKeys = Array.isArray(allowedKeys) ? allowedKeys : [];
  rowAllowedKeys = rowAllowedKeys
    .map((key) => (key == null ? '' : String(key).trim()))
    .filter(Boolean);
  const seenAllowed = new Set();
  const dedupedAllowed = [];
  for (const key of rowAllowedKeys) {
    const lower = key.toLowerCase();
    if (seenAllowed.has(lower)) continue;
    seenAllowed.add(lower);
    dedupedAllowed.push(key);
  }
  rowAllowedKeys = dedupedAllowed;
  for (const baseKey of ['series', 'series_code']) {
    if (!seenAllowed.has(baseKey)) {
      seenAllowed.add(baseKey);
      rowAllowedKeys.push(baseKey);
    }
  }
  const rowAllowedKeySet = new Set(seenAllowed);
  const promptAllowedKeys = Array.from(new Set([
    ...rowAllowedKeys,
    'pn_jp',
    'pn_aliases',
    'ordering_market',
  ]));

  let docai = await processWithDocAI(gcsUri);
  let fullText = docai?.fullText || '';
  if (!fullText) fullText = await parseTextWithPdfParse(gcsUri);

  const typePartMap = extractTypePartPairs(docai?.tables || []);

  const brand = brandHint || (await detectBrandFromText(fullText)) || 'unknown';
  const orderingInfo = extractOrderingInfo(fullText, MAX_PARTS);

  // 코드 후보
  let codes = [];
  if (docai?.tables?.length) codes = codesFromDocAiTables(docai.tables);
  if (!codes.length && fullText) codes = codesFromFreeText(fullText);

  // 표 프리뷰(LLM 컨텍스트)
  let tablePreview = '';
  let perCodePreviewMap = new Map();
  const baseSegments = [];
  if (docai?.tables?.length) {
    baseSegments.push(
      ...docai.tables.slice(0, 6).map((t) => {
        const head = (t.headers || []).join(' | ');
        const rows = (t.rows || []).slice(0, 40).map((r) => r.join(' | ')).join('\n');
        return `HEADER: ${head}\n${rows}`;
      })
    );
    perCodePreviewMap = gatherPerCodeTablePreview(docai.tables, codes.slice(0, MAX_PARTS), {
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
      if (typePartMap.has(norm)) {
        const jpList = typePartMap.get(norm);
        values.pn_jp = Array.isArray(jpList) && jpList.length ? jpList[0] : null;
        values.pn_aliases = jpList && jpList.length ? jpList : null;
        values.ordering_market = 'GLOBAL';
      } else if (!/^[A-Z]HE[0-9]/.test(norm) && /^AHE[0-9]/.test(norm)) {
        values.ordering_market = 'JP';
      }
      for (const key of rowAllowedKeys) {
        if (!key) continue;
        if (Object.prototype.hasOwnProperty.call(values, key) && values[key] != null) {
          row[key] = values[key];
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

  const variantKeys = Object.keys(orderingDomains)
    .map((key) => String(key || '').trim())
    .filter(Boolean);
  if (variantKeys.length) {
    for (const key of variantKeys) {
      const lower = key.toLowerCase();
      if (!rowAllowedKeySet.has(lower)) {
        rowAllowedKeySet.add(lower);
        rowAllowedKeys.push(key);
      }
    }
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
      const values = generated.values && typeof generated.values === 'object' ? generated.values : {};
      const v = hasDocEvidence(normalizeCodeKey(generated.code)) || hasOrderingEvidence(generated.code);
      if (values && typeof values === 'object' && !Object.prototype.hasOwnProperty.call(values, '_pn_template')) {
        values._pn_template = pnTemplate || null;
      }
      pushRow({ code: generated.code, values, brand, verified: v });
    }
    orderingExpanded = out.length > beforeCount;
  }

  if (!out.length) {
    for (const c of seedCodes) {
      pushRow({ code: c, values: {}, brand, verified: hasDocEvidenceValue(c) });
    }
  }

  const tableList = Array.isArray(docai?.tables) ? docai.tables : [];
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
