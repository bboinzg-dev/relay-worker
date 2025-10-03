'use strict';

const { Storage } = require('@google-cloud/storage');
const { batchProcess, extractRelayFields } = require('./src/utils/docai');
const { safeJsonParse } = require('./src/utils/safe-json');

const storage = new Storage();

function parseGs(gsUri) {
  const stripped = String(gsUri).replace('gs://', '');
  const idx = stripped.indexOf('/');
  return { bucket: stripped.slice(0, idx), name: stripped.slice(idx + 1) };
}

(async () => {
  try {
    const input = process.argv[2];
    if (!input) {
      console.error('Usage: node docai_smoke.js gs://bucket/file.pdf');
      process.exit(1);
    }

    const bucket = process.env.GCS_BUCKET_DOCAI || 'partsplan-473810-docai-us';
    const outPrefix = `gs://${bucket}/out/${Date.now()}/`;

    const { outputs } = await batchProcess(input, outPrefix);
    console.log('DocAI outputs:', outputs);

    const first = outputs.find((uri) => uri.endsWith('.json'));
    if (!first) {
      console.error('No JSON outputs found');
      process.exit(3);
    }

    const { bucket: outBucket, name } = parseGs(first);
    const [buf] = await storage.bucket(outBucket).file(name).download();
    const doc = safeJsonParse(buf.toString('utf8'));
    console.log('Extracted:', extractRelayFields(doc));
  } catch (err) {
    console.error(err);
    process.exit(2);
  }
})();
