'use strict';

const { DocumentProcessorServiceClient } = require('@google-cloud/documentai').v1;
const { storage, parseGcsUri } = require('./gcs');

const DEFAULT_PROJECT_ID =
  process.env.DOCAI_PROJECT_ID ||
  process.env.GOOGLE_CLOUD_PROJECT ||
  process.env.GCP_PROJECT_ID ||
  process.env.PROJECT_ID;

const DEFAULT_LOCATION = process.env.DOCAI_LOCATION || process.env.DOCAI_REGION || 'us';
const DEFAULT_PROCESSOR_ID = process.env.DOCAI_PROCESSOR_ID;

const clientsByLocation = new Map();

function ensureTrailingSlash(path) {
  if (!path) throw new Error('path is required');
  return path.endsWith('/') ? path : `${path}/`;
}

function getClient(location) {
  const loc = location || DEFAULT_LOCATION || 'us';
  if (!clientsByLocation.has(loc)) {
    clientsByLocation.set(
      loc,
      new DocumentProcessorServiceClient({ apiEndpoint: `${loc}-documentai.googleapis.com` })
    );
  }
  return clientsByLocation.get(loc);
}

function resolveConfig(overrides = {}) {
  const projectId = overrides.projectId || DEFAULT_PROJECT_ID;
  const location = overrides.location || DEFAULT_LOCATION || 'us';
  const processorId = overrides.processorId || DEFAULT_PROCESSOR_ID;

  if (!projectId) {
    throw new Error(
      'DocAI project ID missing. Set DOCAI_PROJECT_ID or GOOGLE_CLOUD_PROJECT/GCP_PROJECT_ID.'
    );
  }
  if (!processorId) {
    throw new Error('DOCAI_PROCESSOR_ID missing');
  }

  return { projectId, location, processorId };
}

async function batchProcess(gcsInputUri, gcsOutputPrefix, overrides = {}) {
  if (!gcsInputUri) throw new Error('gcsInputUri is required');
  if (!gcsOutputPrefix) throw new Error('gcsOutputPrefix is required');

  const { projectId, location, processorId } = resolveConfig(overrides);
  const client = overrides.client || getClient(location);
  const outputPrefix = ensureTrailingSlash(gcsOutputPrefix);

  const name = `projects/${projectId}/locations/${location}/processors/${processorId}`;
  const request = {
    name,
    inputDocuments: {
      gcsDocuments: { documents: [{ gcsUri: gcsInputUri, mimeType: 'application/pdf' }] },
    },
    documentOutputConfig: {
      gcsOutputConfig: { gcsUri: outputPrefix },
    },
  };

  const [operation] = await client.batchProcessDocuments(request);
  await operation.promise();

  const { bucket, name: prefix } = parseGcsUri(outputPrefix);
  const [files] = await storage.bucket(bucket).getFiles({ prefix });
  const outputs = files
    .map((file) => `gs://${bucket}/${file.name}`)
    .filter((uri) => uri.endsWith('.json'));

  return { operation: operation.name, outputs };
}

function getText(doc, layoutRef) {
  if (!layoutRef || !doc?.text) return null;
  const { textAnchor } = layoutRef;
  if (!textAnchor || !Array.isArray(textAnchor.textSegments)) return null;
  let out = '';
  for (const segment of textAnchor.textSegments) {
    const start = parseInt(segment.startIndex || 0, 10);
    const end = parseInt(segment.endIndex, 10);
    out += doc.text.substring(start, end);
  }
  return out.trim();
}

function normalizeKey(key) {
  return String(key)
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .replace(/[^a-z0-9%./ ]/g, '')
    .trim();
}

function extractKeyMap(doc) {
  const map = {};
  for (const page of doc?.pages || []) {
    for (const field of page.formFields || []) {
      const key = getText(doc, field.fieldName);
      const value = getText(doc, field.fieldValue);
      if (key && value) map[normalizeKey(key)] = value;
    }
  }
  return map;
}

function parseNumber(value) {
  if (!value) return null;
  const match = String(value)
    .replace(/[, ]/g, '')
    .match(/([0-9]+(?:\.[0-9]+)?)/);
  return match ? Number(match[1]) : null;
}

function extractRelayFields(doc) {
  const kv = extractKeyMap(doc);
  const brand = kv['manufacturer'] || kv['brand'] || null;
  const series = kv['series'] || null;
  const code = kv['part number'] || kv['pn'] || kv['model'] || null;
  const contactForm =
    kv['contact form'] || kv['contact configuration'] || kv['form'] || null;
  const contactRatingA = kv['contact rating'] || kv['contact current'] || null;
  const coilV = kv['coil voltage'] || kv['coil rated voltage'] || null;

  return {
    brand,
    series,
    code,
    contact_form: contactForm,
    contact_rating_a: contactRatingA,
    coil_voltage_vdc: parseNumber(coilV),
    dim_l_mm: parseNumber(kv['length'] || kv['dimension l']),
    dim_w_mm: parseNumber(kv['width'] || kv['dimension w']),
    dim_h_mm: parseNumber(kv['height'] || kv['dimension h']),
    contact_rating_text: contactRatingA || null,
    raw_json: doc,
  };
}

module.exports = {
  batchProcess,
  ensureTrailingSlash,
  getText,
  normalizeKey,
  extractKeyMap,
  parseNumber,
  extractRelayFields,
};
