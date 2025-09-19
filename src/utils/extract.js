// relay-worker/src/utils/extract.js
// 하이브리드 OCR:
//  1) Document AI 로 텍스트 추출
//  2) 부족하면 Vertex(Gemini 1.5)로 PDF 자체를 읽어 텍스트 보강
// 그리고 텍스트/파일명 힌트로 brand/series/code 간단 파싱 → extractDataset 제공

'use strict';

const { Storage } = require('@google-cloud/storage');
const { DocumentProcessorServiceClient } = require('@google-cloud/documentai').v1;
const { GoogleAuth } = require('google-auth-library');

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

// ---------- Document AI ----------
async function docaiTextFromBuffer(buf) {
  const project = process.env.DOCAI_PROJECT_ID || process.env.GCP_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT;
  const location = process.env.DOCAI_LOCATION || process.env.DOC_AI_LOCATION || 'us';
  const processorId = process.env.DOCAI_PROCESSOR_ID;

  if (!project || !processorId) {
    console.warn('[extract] DocAI env not set; skipping DocAI.');
    return { text: '', pages: [] };
  }

  const client = new DocumentProcessorServiceClient();
  const name = client.processorPath(project, location, processorId);
  const [res] = await client.processDocument({
    name,
    rawDocument: { content: buf, mimeType: 'application/pdf' },
  });

  const doc = res.document;
  const text = String(doc?.text || '');
  const pages = (doc?.pages || []).map((p, i) => ({
    index: i,
    text: p?.layout?.textAnchor?.content ? String(p.layout.textAnchor.content) : '',
  }));
  return { text, pages };
}

// ---------- Vertex (Gemini 1.5) ----------
async function vertexTextFromBuffer(buf) {
  const project = process.env.GCP_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT;
  const location = process.env.VERTEX_LOCATION || 'us-central1';
  if (!project) {
    console.warn('[extract] Vertex project not set; skipping Vertex.');
    return '';
  }

  const url = `https://${location}-aiplatform.googleapis.com/v1/projects/${project}/locations/${location}/publishers/google/models/gemini-1.5-flash:generateContent`;

  const auth = new GoogleAuth({ scopes: ['https://www.googleapis.com/auth/cloud-platform'] });
  const client = await auth.getClient();
  const token = await client.getAccessToken();
  const accessToken = typeof token === 'string' ? token : token?.token;

  const body = {
    contents: [
      {
        role: 'user',
        parts: [
          { text: '이 PDF의 전체 본문 텍스트를 페이지 순서대로 뽑아 주세요. 표는 탭으로 구분해서 텍스트로 변환해 주세요.' },
          { inlineData: { mimeType: 'application/pdf', data: buf.toString('base64') } },
        ],
      },
    ],
  };

  const r = await fetch(url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });

  if (!r.ok) {
    const t = await r.text().catch(() => '');
    console.warn('[extract] Vertex REST failed', r.status, t.slice(0, 200));
    return '';
  }

  const j = await r.json();
  const parts = j?.candidates?.[0]?.content?.parts || [];
  const text = parts.map((p) => (p.text ? String(p.text) : '')).join('');
  return text || '';
}

/** 하이브리드 텍스트 확보 */
async function extractText(gsUri) {
  const buf = await downloadGcs(gsUri);

  let text = '', pages = [];
  try {
    const d = await docaiTextFromBuffer(buf);
    text = String(d.text || '');
    pages = Array.isArray(d.pages) ? d.pages : [];
  } catch (e) {
    console.warn('[extract] DocAI error:', e?.message || e);
  }

  if (!text || text.length < 500) {
    try {
      const t2 = await vertexTextFromBuffer(buf);
      if (t2 && t2.length > (text?.length || 0)) {
        text = t2;
        pages = []; // Vertex 경로는 페이지 분리 정보 없음
      }
    } catch (e) {
      console.warn('[extract] Vertex fallback error:', e?.message || e);
    }
  }

  return { text, pages };
}

// ---------- 간단 파서(브랜드/코드/시리즈) ----------
const BRAND_PATTERNS = [
  'PANASONIC', 'OMRON', 'TE CONNECTIVITY', 'FINDER', 'HONGFA', 'SCHNEIDER', 'SIEMENS', 'FUJITSU', 'NEXEM', 'MITSUBISHI',
  'PANASONIC ELECTRIC WORKS', 'NAIS', 'PANASONIC INDUSTRY'
];

function detectBrand(text, filename, brandHint) {
  if (brandHint && brandHint.trim()) return brandHint.trim();
  const corpus = `${filename || ''}\n${text || ''}`.toUpperCase();
  for (const b of BRAND_PATTERNS) {
    if (corpus.includes(b)) return b.toLowerCase();
  }
  return 'unknown';
}

function detectSeries(text) {
  if (!text) return null;
  const m = /series\s*[:\-]?\s*([A-Z0-9\-\/ ]{2,40})/i.exec(text);
  return m ? m[1].trim() : null;
}

/** 코드 후보 추출: 대문자/숫자 3~20자, 혼합 토큰 위주 (릴레이 부품명 패턴 근사) */
function findCodes(text) {
  if (!text) return [];
  const tokens = new Set();
  const re = /\b[A-Z]{2,}[A-Z0-9\-\/]{1,18}\b/g; // 예: ALDP112, APAN3124, MY2N-GS 등
  let m;
  while ((m = re.exec(text.toUpperCase()))) {
    const t = m[0];
    if (/^(PAGE|TABLE|FIGURE|INPUT|OUTPUT|VDC|VAC|AC|DC|PDF)$/.test(t)) continue;
    tokens.add(t);
    if (tokens.size > 200) break;
  }
  return Array.from(tokens);
}

function verifiedPagesForCode(pages, code) {
  if (!pages?.length || !code) return [];
  const out = [];
  const K = code.toUpperCase();
  for (const p of pages) {
    if (!p?.text) continue;
    if (p.text.toUpperCase().includes(K)) out.push(p.index);
  }
  return out;
}

/**
 * extractDataset:
 *  - 하이브리드로 text 확보 → brand/series 추정 → code 후보 파싱 → rows 구성
 * @returns {{brand:string, series:string|null, rows:Array<{code:string,series?:string,displayname?:string,verifiedPages?:number[] }>, verifiedPages?:number[], note?:string}}
 */
async function extractDataset({ gcsUri, filename, maxInlinePages = 15, brandHint, codeHint, seriesHint }) {
  const { text, pages } = await extractText(gcsUri);

  const brand = detectBrand(text, filename, brandHint);
  const series = seriesHint || detectSeries(text);

  let codes = findCodes(text);
  if (codeHint && typeof codeHint === 'string') {
    const c = codeHint.trim().toUpperCase();
    if (c) codes = [c, ...codes.filter((x) => x !== c)];
  }

  // rows 구성(상위 64개만)
  const rows = [];
  for (const c of codes.slice(0, 64)) {
    rows.push({
      code: c,
      series: series || null,
      displayname: c,
      verifiedPages: verifiedPagesForCode(pages, c),
    });
  }

  const note = !text
    ? 'empty-text'
    : text.length < 500
    ? 'short-text'
    : undefined;

  return { brand, series: series || null, rows, verifiedPages: [], note };
}

module.exports = { extractText, extractDataset };
