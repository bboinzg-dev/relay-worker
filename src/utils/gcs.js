/* relay-worker/src/utils/gcs.js */
'use strict';

const { Storage } = require('@google-cloud/storage');
const storage = new Storage();

function parseGcsUri(gcsUri) {
  const m = /^gs:\/\/([^/]+)\/(.+)$/.exec(String(gcsUri || ''));
  if (!m) throw new Error('INVALID_GCS_URI');
  return { bucket: m[1], name: m[2] };
}

function canonicalDatasheetPath(bucket, family, brand, code) {
  const bkt = bucket || process.env.GCS_BUCKET?.replace(/^gs:\/\//,'');
  const norm = s => String(s || 'unknown').toLowerCase().replace(/[^a-z0-9_]+/g,'-');
  return `gs://${bkt}/datasheets/${norm(family)}/${norm(brand)}/${norm(code)}.pdf`;
}
function canonicalCoverPath(bucket, family, brand, code) {
  const bkt = bucket || process.env.GCS_BUCKET?.replace(/^gs:\/\//,'');
  const norm = s => String(s || 'unknown').toLowerCase().replace(/[^a-z0-9_]+/g,'-');
  return `gs://${bkt}/covers/${norm(family)}/${norm(brand)}/${norm(code)}.png`;
}

async function getSignedUrl(gcsUri, minutes = 15, action = 'read') {
  const { bucket, name } = parseGcsUri(gcsUri);
  const file = storage.bucket(bucket).file(name);
  const [url] = await file.getSignedUrl({
    version: 'v4',
    action,
    expires: Date.now() + minutes * 60 * 1000,
  });
  return url;
}
async function moveObject(srcGcsUri, dstGcsUri) {
  const { bucket: sb, name: sn } = parseGcsUri(srcGcsUri);
  const { bucket: db, name: dn } = parseGcsUri(dstGcsUri);
  await storage.bucket(sb).file(sn).copy(storage.bucket(db).file(dn));
  await storage.bucket(sb).file(sn).delete({ ignoreNotFound: true });
  return `gs://${db}/${dn}`;
}

module.exports = { storage, parseGcsUri, canonicalDatasheetPath, canonicalCoverPath, getSignedUrl, moveObject };
