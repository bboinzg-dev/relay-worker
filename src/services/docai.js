'use strict';

const path = require('node:path');

let DocumentProcessorServiceClient;
try {
  ({ DocumentProcessorServiceClient } = require('@google-cloud/documentai').v1);
} catch (err) {
  DocumentProcessorServiceClient = null;
}

const { storage, parseGcsUri } = require('../utils/gcs');

const clientsByLocation = new Map();

const HARD_CAP_MS = Number(process.env.EXTRACT_HARD_CAP_MS || 120000);

function withDeadline(promise, ms = HARD_CAP_MS, label = 'op') {
  const timeout = Number.isFinite(ms) && ms > 0 ? ms : HARD_CAP_MS;
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      clearTimeout(timer);
      reject(new Error(`${label}_TIMEOUT`));
    }, timeout);
    Promise.resolve(promise)
      .then((value) => {
        clearTimeout(timer);
        resolve(value);
      })
      .catch((err) => {
        clearTimeout(timer);
        reject(err);
      });
  });
}

function sanitizeRunId(runId) {
  if (!runId) return null;
  const trimmed = String(runId).trim();
  if (!trimmed) return null;
  return trimmed.replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 120) || null;
}

async function waitForJsonOutputs(bucketName, prefix, ms = HARD_CAP_MS) {
  const started = Date.now();
  const bucket = storage.bucket(bucketName);
  while (Date.now() - started < ms) {
    const [files] = await bucket.getFiles({ prefix });
    const jsonFiles = files.filter((file) => file.name && file.name.endsWith('.json'));
    if (jsonFiles.length) return jsonFiles;
    await new Promise((resolve) => setTimeout(resolve, 1500));
  }
  throw new Error('DOCAI_OUTPUT_TIMEOUT');
}

function getClient(location = 'us') {
  if (!DocumentProcessorServiceClient) return null;
  const loc = location || 'us';
  if (!clientsByLocation.has(loc)) {
    clientsByLocation.set(
      loc,
      new DocumentProcessorServiceClient({
        apiEndpoint: `${loc}-documentai.googleapis.com`,
      })
    );
  }
  return clientsByLocation.get(loc);
}

function buildProcessorConfig() {
  const project =
    process.env.DOCAI_PROJECT_ID ||
    process.env.DOC_AI_PROJECT_ID ||
    process.env.GCP_PROJECT_ID ||
    process.env.GOOGLE_CLOUD_PROJECT;
  const location =
    process.env.DOCAI_LOCATION ||
    process.env.DOC_AI_LOCATION ||
    'us';
  const processor = process.env.DOCAI_PROCESSOR_ID;
  if (!project || !processor) return null;
  return {
    name: `projects/${project}/locations/${location}/processors/${processor}`,
    location,
    };
}

function collectText(document = {}) {
  const fullText = document.text || '';
  const tables = [];

  const getText = (layout) => {
    const segments = layout?.textAnchor?.textSegments || [];
    let out = '';
    for (const seg of segments) {
      const start = Number(seg.startIndex || 0);
      const end = Number(seg.endIndex || 0);
      out += fullText.slice(start, end);
    }
    return out.trim();
  };

  for (const page of Array.isArray(document.pages) ? document.pages : []) {
    for (const table of Array.isArray(page.tables) ? page.tables : []) {
      const headers = [];
      const headerRow = table.headerRows?.[0];
      for (const cell of Array.isArray(headerRow?.cells) ? headerRow.cells : []) {
        headers.push(getText(cell.layout));
      }
      const rows = [];
      for (const bodyRow of Array.isArray(table.bodyRows) ? table.bodyRows : []) {
        const row = [];
        for (const cell of Array.isArray(bodyRow.cells) ? bodyRow.cells : []) {
          row.push(getText(cell.layout));
        }
        rows.push(row);
      }
      tables.push({ headers, rows });
    }
  }

  return { text: fullText, tables };
}

async function processDocument(gcsUri, options = {}) {
  const cfg = buildProcessorConfig();
  if (!cfg) return null;
  const client = getClient(cfg.location);
  if (!client) return null;

  const { bucket, name } = parseGcsUri(gcsUri);
  const baseDir = path.posix.dirname(name);
  const baseName = path.posix.basename(name, path.posix.extname(name));
  const safeRunId = sanitizeRunId(options.runId ?? options.run_id);
  const suffix = safeRunId || `${baseName || 'doc'}-${Date.now()}`;
  const outBase = baseDir && baseDir !== '.' ? `${baseDir}/docai/out` : 'docai/out';
  const dir = `${outBase}/${suffix}`;
  const outputPrefix = `gs://${bucket}/${dir}/`;

  const request = {
    name: cfg.name,
    inputDocuments: {
      gcsDocuments: {
        documents: [{ gcsUri, mimeType: 'application/pdf' }],
      },
    },
    documentOutputConfig: {
      gcsOutputConfig: { gcsUri: outputPrefix },
    },
  };

  const [operation] = await client.batchProcessDocuments(request);
  await withDeadline(operation.promise(), HARD_CAP_MS, 'DOCAI_BATCH');

  const { bucket: outBucket, name: outPrefix } = parseGcsUri(outputPrefix);
  const jsonFiles = await waitForJsonOutputs(outBucket, outPrefix, HARD_CAP_MS);
  for (const file of jsonFiles) {
    try {
      const [buf] = await file.download();
      const parsed = JSON.parse(buf.toString('utf8'));
      const document = parsed?.document || parsed;
      if (document?.text) {
        return collectText(document);
      }
    } catch (err) {
      console.warn('[DocAI] failed to parse output', err?.message || err);
    }
  }
  return null;
}

module.exports = { processDocument };