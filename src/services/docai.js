'use strict';

let DocumentProcessorServiceClient;
try {
  ({ DocumentProcessorServiceClient } = require('@google-cloud/documentai').v1);
} catch (err) {
  DocumentProcessorServiceClient = null;
}

const { storage, parseGcsUri } = require('../utils/gcs');

let cachedClient = null;

function getClient() {
  if (!DocumentProcessorServiceClient) return null;
  if (!cachedClient) {
    cachedClient = new DocumentProcessorServiceClient();
  }
  return cachedClient;
}

function buildProcessorName() {
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

async function processDocument(gcsUri) {
  const cfg = buildProcessorName();
  if (!cfg) return null;
  const client = getClient();
  if (!client) return null;

  const { bucket, name } = parseGcsUri(gcsUri);
  const [buf] = await storage.bucket(bucket).file(name).download();

  const request = {
    name: cfg.name,
    rawDocument: { content: buf, mimeType: 'application/pdf' },
    skipHumanReview: true,
  };

  const [result] = await client.processDocument(request);
  const document = result?.document;
  if (!document) return null;
  return collectText(document);
}

module.exports = { processDocument };