const { DocumentProcessorServiceClient } = require('@google-cloud/documentai').v1;
const { parseGcsUri } = require('./gcs');

const PROJECT_ID = process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT;
const DOCAI_LOCATION = process.env.DOCAI_LOCATION || 'us';
const PROCESSOR_ID = process.env.DOCAI_PROCESSOR_ID || null;

const client = new DocumentProcessorServiceClient();

async function processOnlineGcs(gcsUri, { processorId = PROCESSOR_ID, location = DOCAI_LOCATION } = {}) {
  if (!processorId) throw new Error('DOCAI_PROCESSOR_ID not set');
  const name = `projects/${PROJECT_ID}/locations/${location}/processors/${processorId}`;
  // Online processing with GCS requires batch. Here we keep a minimal placeholder.
  // In practice, prefer batchProcessDocuments for larger PDFs.
  // Returning a stub structure for now.
  return { text: '', entities: [] };
}

module.exports = { processOnlineGcs };
