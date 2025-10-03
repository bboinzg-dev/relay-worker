const {DocumentProcessorServiceClient} = require('@google-cloud/documentai').v1;
const {Storage} = require('@google-cloud/storage');

const PROJECT  = process.env.GOOGLE_CLOUD_PROJECT || process.env.GCP_PROJECT_ID;
const LOCATION = process.env.DOC_AI_LOCATION || process.env.DOC_AI_REGION || 'us';
const PROCESSOR_ID = process.env.DOCAI_PROCESSOR_ID;

function ensureSlash(p){ return p.endsWith('/') ? p : p + '/'; }
function parseGcsUri(uri){
  if(!uri.startsWith('gs://')) throw new Error('gcs uri must start with gs://');
  const s = uri.replace('gs://',''); const i = s.indexOf('/');
  return { bucket: s.slice(0,i), prefix: s.slice(i+1) };
}

async function batchProcess(gcsInputUri, gcsOutputPrefix){
  if(!PROJECT) throw new Error('GOOGLE_CLOUD_PROJECT/GCP_PROJECT_ID missing');
  if(!PROCESSOR_ID) throw new Error('DOCAI_PROCESSOR_ID missing');

  const client = new DocumentProcessorServiceClient({ apiEndpoint: `${LOCATION}-documentai.googleapis.com` });
  const name = `projects/${PROJECT}/locations/${LOCATION}/processors/${PROCESSOR_ID}`;
  const request = {
    name,
    inputDocuments: { gcsDocuments: { documents: [{ gcsUri: gcsInputUri, mimeType: 'application/pdf' }] } },
    documentOutputConfig: { gcsOutputConfig: { gcsUri: ensureSlash(gcsOutputPrefix) } },
  };

  const [op] = await client.batchProcessDocuments(request);
  await op.promise();

  const storage = new Storage();
  const {bucket, prefix} = parseGcsUri(ensureSlash(gcsOutputPrefix));
  const [files] = await storage.bucket(bucket).getFiles({ prefix });
  const jsonFiles = files.map(f => `gs://${bucket}/${f.name}`).filter(u => u.endsWith('.json'));
  return { operation: op.name, outputs: jsonFiles };
}

module.exports = { batchProcess };
