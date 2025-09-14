const { Storage } = require('@google-cloud/storage');

const storage = new Storage(); // relies on ADC or Workload Identity
function parseGcsUri(gcsUri) {
  if (!gcsUri?.startsWith('gs://')) throw new Error('gcsUri must start with gs://');
  const [, , bucket, ...rest] = gcsUri.split('/');
  return { bucket, name: rest.join('/') };
}

async function getSignedUrl(gcsUri, minutes=15, action='read') {
  const { bucket, name } = parseGcsUri(gcsUri);
  const [url] = await storage.bucket(bucket).file(name).getSignedUrl({
    action,
    version: 'v4',
    expires: Date.now() + minutes * 60 * 1000,
  });
  return url;
}

module.exports = { storage, parseGcsUri, getSignedUrl };
