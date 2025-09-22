// src/utils/extract.js
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

// DocAI online imageless: 30p까지 허용
async function docaiProcessImageless(gsUri) {
  const project   = process.env.DOCAI_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT;
  const location  = process.env.DOCAI_LOCATION || 'us';
  const processor = process.env.DOCAI_PROCESSOR_ID;
  if (!project || !processor) throw new Error('DocAI env missing');

  const client = new DocumentProcessorServiceClient({ apiEndpoint: `${location}-documentai.googleapis.com` });
  const name = `projects/${project}/locations/${location}/processors/${processor}`;

  const rawDocument = { content: await downloadGcs(gsUri), mimeType: 'application/pdf' };
  const [result] = await client.processDocument({ name, rawDocument, imagelessMode: true, skipHumanReview: true });
  const doc = result?.document;
  return { text: doc?.text || '', pages: doc?.pages || [] };
}

async function extractText(gsUri) {
  // 1) DocAI imageless 우선
  try {
    const r = await docaiProcessImageless(gsUri);
    if (r?.text && r.text.length > 200) return r;
  } catch (e) {
    console.warn('[extractText] DocAI imageless failed:', e?.message || e);
  }

  // 2) 폴백: 필요 시 다른 경로 추가
  return { text: '', pages: [] };
}

module.exports = { extractText, parseGsUri };
