'use strict';

const { Storage } = require('@google-cloud/storage');
const { DocumentProcessorServiceClient } = require('@google-cloud/documentai').v1;

let pdfParse;
try { pdfParse = require('pdf-parse'); } catch (_) { pdfParse = null; }

const storage = new Storage();

const EXTRACT_TIMEOUT_MS = Number(process.env.EXTRACT_HARD_CAP_MS || 60_000);
const INLINE_PAGE_LIMIT = Number(process.env.MAX_DOC_PAGES_INLINE || 15);

function parseGsUri(gsUri) {
  const m = String(gsUri).match(/^gs:\/\/([^/]+)\/(.+)$/);
  if (!m) throw new Error('invalid gcs uri: ' + gsUri);
  return { bucket: m[1], name: m[2] };
}

async function downloadGcs(gsUri) {
  const { bucket, name } = parseGsUri(gsUri);
  const [buf] = await storage.bucket(bucket).file(name).download();
  return buf;
}

function samplePages(total, k) {
  if (!Number.isFinite(total) || total <= 0) return Array.from({ length: k }, (_, i) => i + 1);
  if (total <= k) return Array.from({ length: total }, (_, i) => i + 1);
  const pick = new Set([1, 2, 3, total - 1, total]);
  while (pick.size < k) {
    const pos = Math.max(1, Math.min(total, Math.round((pick.size / (k + 1)) * total)));
    pick.add(pos);
  }
  return Array.from(pick).sort((a, b) => a - b).slice(0, k);
}

function withTimeout(promise, timeoutMs, label) {
  const ms = Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : EXTRACT_TIMEOUT_MS;
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      const t = setTimeout(() => {
        clearTimeout(t);
        reject(new Error(`${label || 'operation'}_timeout`));
      }, ms);
    }),
  ]);
}

async function tryInlineExtract(gsUri, { maxPages = INLINE_PAGE_LIMIT } = {}) {
  let pages = 0;
  if (!gsUri) return { text: '', pages, note: 'missing_uri' };

  try {
    const buf = await downloadGcs(gsUri);
    if (!pdfParse) {
      return { text: '', pages, note: 'pdf_parse_unavailable' };
    }

    const parsed = await withTimeout(pdfParse(buf), EXTRACT_TIMEOUT_MS, 'pdf_parse');
    const pageCount = Number(parsed?.numpages || 0);
    pages = Number.isFinite(pageCount) ? pageCount : 0;

    const raw = String(parsed?.text || '');
    if (!raw) {
      return { text: '', pages, note: 'empty-inline' };
    }

    const limit = Number.isFinite(maxPages) && maxPages > 0 ? maxPages : INLINE_PAGE_LIMIT;
    if (!limit || !pages || pages <= limit) {
      return { text: raw, pages, note: 'inline_pdf_parse' };
    }

    const segments = raw.split('\f');
    const clipped = segments.slice(0, limit).join('\n');
    return { text: clipped || raw, pages, note: 'inline_pdf_parse_truncated' };
  } catch (err) {
    return { text: '', pages, note: `inline_error:${err?.message || err}` };
  }
}

// DocAI online imageless: 30p까지, 초과 에러 시 15p 샘플링
async function docaiProcessSmart(gsUri) {
  const project = process.env.DOCAI_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT;
  const location = process.env.DOCAI_LOCATION || 'us';
  const processor = process.env.DOCAI_PROCESSOR_ID;
  if (!project || !processor) throw new Error('DocAI env missing');

  const client = new DocumentProcessorServiceClient({ apiEndpoint: `${location}-documentai.googleapis.com` });
  const name = `projects/${project}/locations/${location}/processors/${processor}`;

  const buf = await downloadGcs(gsUri);
  try {
    const [r] = await client.processDocument({
      name,
      rawDocument: { content: buf, mimeType: 'application/pdf' },
      imagelessMode: true,
      skipHumanReview: true,
    });
    return { text: r?.document?.text || '', pages: r?.document?.pages || [], note: 'docai_imageless' };
  } catch (e) {
    const msg = String(e?.message || e);
    if (!/supports up to 15 pages|exceed the limit: 15/i.test(msg)) throw e;
    let total = 0;
    try {
      if (pdfParse) {
        const parsed = await pdfParse(buf);
        total = Number(parsed?.numpages || 0);
      }
    } catch (_) {}
    const pages = samplePages(total || 30, 15);
    const [r2] = await client.processDocument({
      name,
      rawDocument: { content: buf, mimeType: 'application/pdf' },
      processOptions: { individualPageSelector: { pages } },
      skipHumanReview: true,
    });
    return { text: r2?.document?.text || '', pages: r2?.document?.pages || [], note: 'docai_sampled' };
  }
}

async function processWithDocAI(gsUri, { timeoutMs = EXTRACT_TIMEOUT_MS } = {}) {
  return withTimeout(docaiProcessSmart(gsUri), timeoutMs, 'docai_process');
}

async function extractText(gsUri) {
  const inline = await tryInlineExtract(gsUri, { maxPages: INLINE_PAGE_LIMIT });
  const textContent = inline?.text || '';
  if (!textContent || textContent.length < 1000 || /empty-text|no_rows_extracted/i.test(String(inline?.note || ''))) {
    return await processWithDocAI(gsUri, { timeoutMs: EXTRACT_TIMEOUT_MS });
  }
  return inline;
}

module.exports = { extractText, parseGsUri, tryInlineExtract, processWithDocAI };
