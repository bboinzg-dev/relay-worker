// relay-worker/src/utils/extract.js
'use strict';

/**
 * Hybrid extractor
 *  - Vertex: pick pages that look important (TYPES/ORDERING/SPECS)
 *  - Document AI:
 *      * online (≤15 pages): process only selected pages
 *      * batch (>15 or when needed): write results to GCS and read back
 * Return: { text: string, pages: [{ page: number, text: string }...] }
 */

const { Storage } = require('@google-cloud/storage');
const storage = new Storage();
const path = require('path');

const DOCAI_PROJECT_ID   = process.env.DOCAI_PROJECT_ID || process.env.GCP_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT;
const DOCAI_LOCATION     = process.env.DOCAI_LOCATION   || process.env.DOC_AI_LOCATION || 'us';
const DOCAI_PROCESSOR_ID = process.env.DOCAI_PROCESSOR_ID || '';
const DOCAI_OUTPUT_BUCKET= (process.env.DOCAI_OUTPUT_BUCKET || '').replace(/^gs:\/\//,''); // e.g. partsplan-docai-us/docai-out
const MAX_INLINE         = Number(process.env.MAX_DOC_PAGES_INLINE || 15);

// ---------- utils ----------
function isGsUri(u){ return typeof u === 'string' && /^gs:\/\//i.test(u); }
function gsParse(u){
  const m = /^gs:\/\/([^/]+)\/(.+)$/.exec(String(u||''));
  if (!m) throw new Error('INVALID_GCS_URI');
  return { bucket: m[1], object: m[2] };
}
function joinText(pages = []) {
  return pages.map(p => p.text || '').filter(Boolean).join('\n\n');
}

// ---------- Vertex page picker ----------
async function vertexPickPages(gcsUri) {
  try {
    const { callModelJson } = require('./vertex');
    const sys = [
      'You are a document page ranker.',
      'From the given PDF, select up to 12 page numbers that likely contain:',
      '- TYPES / TYPE(S) TABLE listing part numbers',
      '- ORDERING INFORMATION tables with enumerated items (not pure combination rules)',
      '- key specifications tables',
      'Return strict JSON {"pages":[1,3,5,...]} (1-indexed).',
      'Do not return more than 12 pages.',
    ].join('\n');

    const usr = JSON.stringify({ gcs_uri: gcsUri, hint: ['TYPES','ORDERING','SPECIFICATIONS'] });

    const out = await callModelJson(sys, usr, { maxOutputTokens: 1024 });
    let arr = Array.isArray(out?.pages) ? out.pages : [];
    arr = arr.map(n => Number(n)).filter(n => Number.isInteger(n) && n > 0);
    // unique, sorted, <=12
    const uniq = [...new Set(arr)].sort((a,b)=>a-b).slice(0,12);
    return uniq;
  } catch (e) {
    console.warn('[extract] vertexPickPages WARN:', e?.message || e);
    return [];
  }
}

// ---------- Document AI online ----------
async function docaiOnlineProcess({ gcsUri, pages = [] }) {
  const { DocumentProcessorServiceClient } = require('@google-cloud/documentai').v1;
  const client = new DocumentProcessorServiceClient();

  if (!DOCAI_PROCESSOR_ID) throw new Error('DOCAI_PROCESSOR_ID not set');

  const name = client.processorPath(DOCAI_PROJECT_ID, DOCAI_LOCATION, DOCAI_PROCESSOR_ID);

  // Online ProcessRequest
  // 일부 SDK 버전에서 individualPageSelector가 없을 수 있으니 try-catch로 보완
  const request = { name, inputDocuments: undefined, rawDocument: undefined, // online with GCS input is not supported
    // online은 rawDocument (바이너리) 또는 inlineDocument 로 가야 함 → 바이너리 다운로드
  };

  // 1) Download PDF binary from GCS
  const { bucket, object } = gsParse(gcsUri);
  const [buf] = await storage.bucket(bucket).file(object).download();

  let processRequest;
  try {
    // v1 supports: rawDocument
    processRequest = {
      name,
      rawDocument: { content: buf, mimeType: 'application/pdf' },
      processOptions: pages?.length ? { individualPageSelector: { pages: pages.map(String) } } : {}
    };
  } catch (_) {
    // fallback: without page selector
    processRequest = {
      name,
      rawDocument: { content: buf, mimeType: 'application/pdf' }
    };
  }

  const [result] = await client.processDocument(processRequest);
  const doc = result.document;
  const outPages = (doc?.pages || []).map((p, i) => ({
    page: p.pageNumber || (pages?.[i] || i + 1),
    text: p.layout?.textAnchor?.content || p.layout?.text || ''
  }));
  return { pages: outPages, text: joinText(outPages) };
}

// ---------- Document AI batch ----------
async function docaiBatchProcess({ gcsUri, pages = [] }) {
  const { DocumentProcessorServiceClient } = require('@google-cloud/documentai').v1;
  const client = new DocumentProcessorServiceClient();

  if (!DOCAI_PROCESSOR_ID) throw new Error('DOCAI_PROCESSOR_ID not set');
  if (!DOCAI_OUTPUT_BUCKET) throw new Error('DOCAI_OUTPUT_BUCKET not set');

  const name = client.processorPath(DOCAI_PROJECT_ID, DOCAI_LOCATION, DOCAI_PROCESSOR_ID);

  const { bucket, object } = gsParse(gcsUri);
  const outputPrefix = path.posix.join(DOCAI_OUTPUT_BUCKET.replace(/^\//,''), Date.now().toString());

  const request = {
    name,
    inputDocuments: {
      gcsDocuments: {
        documents: [{ gcsUri, mimeType: 'application/pdf' }]
      }
    },
    documentOutputConfig: {
      gcsOutputConfig: { gcsUri: `gs://${outputPrefix}/` }
    }
    // pageRanges 등은 SDK/버전에 따라 다를 수 있어 안전하게 전체 처리
  };

  const [operation] = await client.batchProcessDocuments(request);
  await operation.promise();

  // Read the first JSON from the output prefix
  const [files] = await storage.bucket(DOCAI_OUTPUT_BUCKET.split('/')[0])
    .getFiles({ prefix: DOCAI_OUTPUT_BUCKET.split('/')[1] ? `${DOCAI_OUTPUT_BUCKET.split('/')[1]}/` : '' });

  // pick only files under this batch outputPrefix
  const outputs = files.filter(f => f.name.startsWith(outputPrefix));
  const jsonFiles = outputs.filter(f => f.name.endsWith('.json'));

  const pagesOut = [];
  for (const jf of jsonFiles) {
    const [buf] = await jf.download();
    const parsed = JSON.parse(buf.toString('utf8'));
    const doc = parsed.document || parsed;
    const ps = (doc.pages || []).map(p => ({
      page: p.pageNumber || pages?.[pages?.length - 1] || 0,
      text: p.layout?.textAnchor?.content || p.layout?.text || ''
    }));
    pagesOut.push(...ps);
  }
  return { pages: pagesOut, text: joinText(pagesOut) };
}

// ---------- entry point ----------
async function extractText(gcsUri) {
  if (!isGsUri(gcsUri)) throw new Error('gcsUri must be gs://…');

  // 1) Vertex로 "중요 페이지" pick (최대 12p)
  const picked = await vertexPickPages(gcsUri);

  // 2) 온라인 조건: picked가 있고, 그 길이가 1~15p 범위
  if (picked.length > 0 && picked.length <= MAX_INLINE) {
    try {
      return await docaiOnlineProcess({ gcsUri, pages: picked });
    } catch (e) {
      console.warn('[extract] docaiOnlineProcess WARN:', e?.message || e);
    }
  }

  // 3) 그렇지 않으면 전체/대용량 처리: 배치
  try {
    return await docaiBatchProcess({ gcsUri });
  } catch (e) {
    console.warn('[extract] docaiBatchProcess WARN:', e?.message || e);
  }

  // 4) 모두 실패 → 최소한의 폴백(빈 텍스트)
  return { text: '', pages: [] };
}

module.exports = { extractText };
