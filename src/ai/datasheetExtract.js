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

const db = require('../utils/db');
const { parseGcsUri } = require('../utils/gcs');
const { safeJsonParse } = require('../utils/safe-json');

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
  if (!/^[A-Za-z0-9][A-Za-z0-9\-_/\.]{2,}$/.test(s)) return false;
  if (!/\d/.test(s)) return false;
  if (/\b(OHM|Ω|VDC|VAC|AMP|A|V|W|HZ|MS|SEC|UL|ROHS|REACH|DATE|PAGE)\b/i.test(s)) return false;
  return true;
}
function mapHeaderToKey(h) {
  const s = String(h || '').trim().toLowerCase().replace(/\s+/g, ' ');
  if (/\b(part( no\.?| number)|model|type|ordering code|catalog( no\.?| number))\b/.test(s)) return 'code';
  return null;
}
function codesFromDocAiTables(tables) {
  const set = new Set();
  for (const t of (tables || [])) {
    const idx = {};
    (t.headers || []).forEach((h, i) => { const k = mapHeaderToKey(h); if (k) idx[k] = i; });
    if (idx.code == null) continue;
    for (const r of (t.rows || [])) {
      const code = String(r[idx.code] || '').trim().toUpperCase();
      if (looksLikeCode(code)) set.add(code);
    }
  }
  return Array.from(set);
}
function codesFromFreeText(txt) {
  const set = new Set();
  const re = /\b([A-Z]{2,6}[A-Z0-9\-_/\.]{2,})\b/g;
  let m; while ((m = re.exec(txt)) !== null) {
    const cand = m[1].toUpperCase();
    if (looksLikeCode(cand)) set.add(cand);
    if (set.size > MAX_PARTS) break;
  }
  return Array.from(set);
}

/* -------------------- Gemini mapping -------------------- */
async function geminiMapValues({ family, brandHint, codes, allowedKeys, docText, tablePreview }) {
  const { project, location, model } = resolveGemini();
  if (!VertexAI || !project) return [];

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
    generationConfig: { temperature: 0.2, responseMimeType: 'application/json', maxOutputTokens: 8192 },
  });

  let txt = resp?.response?.candidates?.[0]?.content?.parts?.[0]?.text || '{}';
  try {
    const parsed = safeJsonParse(txt);
    return Array.isArray(parsed?.parts) ? parsed.parts : [];
  } catch {
    return [];
  }
}

/* -------------------- Public: main extractor -------------------- */
async function extractPartsAndSpecsFromPdf({ gcsUri, allowedKeys, brandHint = null }) {
  let docai = await processWithDocAI(gcsUri);
  let fullText = docai?.fullText || '';
  if (!fullText) fullText = await parseTextWithPdfParse(gcsUri);

  const brand = brandHint || (await detectBrandFromText(fullText)) || 'unknown';

  // 코드 후보
  let codes = [];
  if (docai?.tables?.length) codes = codesFromDocAiTables(docai.tables);
  if (!codes.length && fullText) codes = codesFromFreeText(fullText);

  // 표 프리뷰(LLM 컨텍스트)
  let tablePreview = '';
  if (docai?.tables?.length) {
    tablePreview = docai.tables.slice(0, 6).map(t => {
      const head = (t.headers || []).join(' | ');
      const rows = (t.rows || []).slice(0, 40).map(r => r.join(' | ')).join('\n');
      return `HEADER: ${head}\n${rows}`;
    }).join('\n---\n');
  }

  // Gemini로 values 매핑(블루프린트 키만)
  const mapped = await geminiMapValues({
    family: null,
    brandHint: brand,
    codes: codes.slice(0, MAX_PARTS),
    allowedKeys,
    docText: fullText,
    tablePreview
  });

  // 병합
  const out = [];
  const seen = new Set();
  for (const m of mapped) {
    const code = String(m?.code || '').trim().toUpperCase();
    if (!code || seen.has(code)) continue;
    seen.add(code);
    const row = { code, verified_in_doc: true };
    if (m?.brand) row.brand = m.brand;
    const values = m?.values || {};
    for (const k of allowedKeys) { if (values[k] != null) row[k] = values[k]; }
    out.push(row);
  }

  if (!out.length) for (const c of codes.slice(0, MAX_PARTS)) out.push({ code: c, verified_in_doc: true });

  return { brand, rows: out.slice(0, MAX_PARTS), text: fullText };
}

module.exports = { extractPartsAndSpecsFromPdf };
