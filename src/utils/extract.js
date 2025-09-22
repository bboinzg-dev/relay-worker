'use strict';

const { Storage } = require('@google-cloud/storage');
const { DocumentProcessorServiceClient } = require('@google-cloud/documentai').v1;

const storage = new Storage();

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
  if (!Number.isFinite(total) || total <= 0) return Array.from({ length: k }, (_,i)=>i+1);
  if (total <= k) return Array.from({ length: total }, (_,i)=>i+1);
  const pick = new Set([1,2,3, total-1, total]);
  while (pick.size < k) {
    const pos = Math.max(1, Math.min(total, Math.round((pick.size/(k+1))*total)));
    pick.add(pos);
  }
  return Array.from(pick).sort((a,b)=>a-b).slice(0,k);
}

// DocAI online imageless: 30p까지, 초과 에러 시 15p 샘플링
async function docaiProcessSmart(gsUri) {
  const project   = process.env.DOCAI_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT;
  const location  = process.env.DOCAI_LOCATION || 'us';
  const processor = process.env.DOCAI_PROCESSOR_ID;
  if (!project || !processor) throw new Error('DocAI env missing');

  const client = new DocumentProcessorServiceClient({ apiEndpoint: `${location}-documentai.googleapis.com` });
  const name = `projects/${project}/locations/${location}/processors/${processor}`;

  const buf = await downloadGcs(gsUri);
  try {
    const [r] = await client.processDocument({ name, rawDocument: { content: buf, mimeType: 'application/pdf' }, imagelessMode: true, skipHumanReview: true });
    return { text: r?.document?.text || '', pages: r?.document?.pages || [] };
  } catch (e) {
    const msg = String(e?.message || e);
    if (!/supports up to 15 pages|exceed the limit: 15/i.test(msg)) throw e;
    let total = 0;
    try { const pdf = require('pdf-parse'); const p = await pdf(buf); total = Number(p?.numpages || 0); } catch {}
    const pages = samplePages(total || 30, 15);
    const [r2] = await client.processDocument({
      name,
      rawDocument: { content: buf, mimeType: 'application/pdf' },
      processOptions: { individualPageSelector: { pages } },
      skipHumanReview: true,
    });
    return { text: r2?.document?.text || '', pages: r2?.document?.pages || [] };
  }
}

async function extractText(gsUri) {
  return await docaiProcessSmart(gsUri);
}

module.exports = { extractText, parseGsUri };
