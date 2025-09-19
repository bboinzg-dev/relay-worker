const { DocumentProcessorServiceClient } = require('@google-cloud/documentai').v1;
const { parseGcsUri } = require('./gcs');

 const env = require('../config/env');
 const project = env.DOCAI_PROJECT_ID;
 const location = env.DOCAI_LOCATION;
 const processorId = env.DOCAI_PROCESSOR_ID;

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
