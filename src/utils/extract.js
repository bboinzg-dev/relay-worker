'use strict';

const { Storage } = require('@google-cloud/storage');
const { DocumentProcessorServiceClient } = require('@google-cloud/documentai').v1;
const { GoogleAuth } = require('google-auth-library');
const env = require('../config/env');

const storage = new Storage();

function parseGsUri(gsUri) {
  const m = /^gs:\/\/([^/]+)\/(.+)$/.exec(String(gsUri || ''));
  if (!m) throw new Error(`invalid gcs uri: ${gsUri}`);
  return { bucket: m[1], name: m[2] };
}
async function downloadGcs(gsUri) {
  const { bucket, name } = parseGsUri(gsUri);
  const [buf] = await storage.bucket(bucket).file(name).download();
  return buf;
}

/** 1) DocAI 텍스트 추출 */
async function docaiTextFromBuffer(buf) {
  const project = env.DOCAI_PROJECT_ID;
  const location = env.DOCAI_LOCATION;
  const processorId = env.DOCAI_PROCESSOR_ID;
  if (!project || !processorId) return { text: '', pages: [] };

  const client = new DocumentProcessorServiceClient();
  const name = client.processorPath(project, location, processorId);
  const [res] = await client.processDocument({
    name, rawDocument: { content: buf, mimeType: 'application/pdf' },
  });

  const doc = res.document;
  const text = String(doc?.text || '');
  const pages = (doc?.pages || []).map((p, i) => ({
    index: i,
    text: p?.layout?.textAnchor?.content ? String(p.layout.textAnchor.content) : '',
  }));
  return { text, pages };
}

/** 2) Vertex(1.5-flash)로 PDF 자체를 읽어 텍스트 확보 (DocAI 부족 시) */
async function vertexTextFromBuffer(buf) {
  const project = env.PROJECT_ID;
  const location = env.VERTEX_LOCATION || 'us-central1';
  if (!project) return '';

  const url = `https://${location}-aiplatform.googleapis.com/v1/projects/${project}/locations/${location}/publishers/google/models/gemini-1.5-flash:generateContent`;
  const auth = new GoogleAuth({ scopes: ['https://www.googleapis.com/auth/cloud-platform'] });
  const client = await auth.getClient();
  const token = await client.getAccessToken();
  const accessToken = typeof token === 'string' ? token : token?.token;

  const body = {
    contents: [{
      role: 'user',
      parts: [
        { text: '이 PDF의 전체 본문 텍스트를 페이지 순서대로 뽑아 주세요. 표는 탭으로 구분해서 텍스트로 변환해 주세요.' },
        { inlineData: { mimeType: 'application/pdf', data: buf.toString('base64') } },
      ],
    }],
  };

  const r = await fetch(url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    const t = await r.text().catch(() => '');
    console.warn('[extractText] Vertex REST failed', r.status, t.slice(0, 160));
    return '';
  }
  const j = await r.json();
  const parts = j?.candidates?.[0]?.content?.parts || [];
  const text = parts.map(p => (p.text ? String(p.text) : '')).join('');
  return text || '';
}

/** 하이브리드: DocAI → (짧으면) Vertex */
async function extractText(gsUri) {
  const buf = await downloadGcs(gsUri);

  let text = '', pages = [];
  try {
    const d = await docaiTextFromBuffer(buf);
    text = String(d.text || '');
    pages = Array.isArray(d.pages) ? d.pages : [];
  } catch (e) {
    console.warn('[extractText] DocAI error:', e?.message || e);
  }

  if (!text || text.length < 500) {
    try {
      const t2 = await vertexTextFromBuffer(buf);
      if (t2 && t2.length > (text?.length || 0)) {
        text = t2; pages = [];
      }
    } catch (e) {
      console.warn('[extractText] Vertex fallback error:', e?.message || e);
    }
  }
  return { text, pages };
}

/* ───── 간단 파서(브랜드/시리즈/코드 후보) ───── */
const BRAND_PATTERNS = [
  'PANASONIC','OMRON','TE CONNECTIVITY','FINDER','HONGFA',
  'SCHNEIDER','SIEMENS','FUJITSU','NEXEM','MITSUBISHI','NAIS','PANASONIC ELECTRIC WORKS'
];
function detectBrand(text, filename, brandHint) {
  if (brandHint && brandHint.trim()) return brandHint.trim();
  const corpus = `${filename || ''}\n${text || ''}`.toUpperCase();
  for (const b of BRAND_PATTERNS) if (corpus.includes(b)) return b.toLowerCase();
  return 'unknown';
}
function detectSeries(text) {
  if (!text) return null;
  const m = /series\s*[:\-]?\s*([A-Z0-9\-\/ ]{2,40})/i.exec(text);
  return m ? m[1].trim() : null;
}
function findCodes(text) {
  if (!text) return [];
  const tokens = new Set();
  const re = /\b[A-Z]{2,}[A-Z0-9\-\/]{1,18}\b/g;
  let m; while ((m = re.exec(text.toUpperCase()))) {
    const t = m[0];
    if (/^(PAGE|TABLE|FIGURE|INPUT|OUTPUT|VDC|VAC|AC|DC|PDF)$/.test(t)) continue;
    tokens.add(t);
    if (tokens.size > 200) break;
  }
  return Array.from(tokens);
}
function verifiedPagesForCode(pages, code) {
  if (!pages?.length || !code) return [];
  const K = code.toUpperCase();
  const out = [];
  for (const p of pages) if (p?.text && p.text.toUpperCase().includes(K)) out.push(p.index);
  return out;
}

/** 외부에서 쓰는 API: 데이터셋 추출 */
async function extractDataset({ gcsUri, filename, maxInlinePages = 15, brandHint, codeHint, seriesHint }) {
  const { text, pages } = await extractText(gcsUri);
  const brand = detectBrand(text, filename, brandHint);
  const series = seriesHint || detectSeries(text);

  let codes = findCodes(text);
  if (codeHint && typeof codeHint === 'string') {
    const c = codeHint.trim().toUpperCase();
    if (c) codes = [c, ...codes.filter(x => x !== c)];
  }

  const rows = codes.slice(0, 64).map(c => ({
    code: c,
    series: series || null,
    displayname: c,
    verifiedPages: verifiedPagesForCode(pages, c),
  }));

  const note = !text ? 'empty-text' : (text.length < 500 ? 'short-text' : undefined);
  return { brand, series: series || null, rows, verifiedPages: [], note };
}

module.exports = { extractText, extractDataset };
