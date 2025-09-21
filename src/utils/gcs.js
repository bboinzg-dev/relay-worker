// src/utils/gcs.js
const { Storage } = require('@google-cloud/storage');

const storage = new Storage();

/** Parse a GCS URI "gs://bucket/path/to/object" into { bucket, name } */
function parseGcsUri(gcsUri) {
  if (!gcsUri || !/^gs:\/\//.test(gcsUri)) throw new Error('gcsUri must start with gs://');
  const u = String(gcsUri).replace(/^gs:\/\//, '');
  const slash = u.indexOf('/');
  if (slash < 0) return { bucket: u, name: '' };
  return { bucket: u.slice(0, slash), name: u.slice(slash + 1) };
}

async function readText(gcsUri, limitBytes=4*1024*1024) {
  const { bucket, name } = parseGcsUri(gcsUri);
  const [buf] = await storage.bucket(bucket).file(name).download({ start: 0, end: limitBytes });
  return buf.toString('utf8');
}

async function getSignedUrl(gcsUri, minutes = 15, action = 'read') {
  const { bucket, name } = parseGcsUri(gcsUri);
  const [url] = await storage.bucket(bucket).file(name).getSignedUrl({
    action: action === 'write' ? 'write' : 'read',
    expires: Date.now() + minutes * 60 * 1000,
  });
  return url;
}

function sanitizeId(s) {
  return String(s || '').toLowerCase().replace(/[^a-z0-9._-]/g, '-');
}

function canonicalDatasheetPath(bucket, family, brand, code) {
  const b = sanitizeId(brand);
  const c = sanitizeId(code);
  const f = sanitizeId(family);
  return `gs://${bucket}/datasheets/${f}/${b}/${c}/datasheet.pdf`;
}

function canonicalCoverPath(bucket, family, brand, code) {
  const b = sanitizeId(brand);
  const c = sanitizeId(code);
  const f = sanitizeId(family);
  return `gs://${bucket}/images/${f}/${b}/${c}/cover.png`;
}

async function moveObject(srcGcsUri, dstGcsUri) {
  const src = parseGcsUri(srcGcsUri);
  const dst = parseGcsUri(dstGcsUri);
  await storage.bucket(src.bucket).file(src.name).move(storage.bucket(dst.bucket).file(dst.name));
  return dstGcsUri;
}

module.exports = {
  storage,
  parseGcsUri,
  readText,
  getSignedUrl,
  canonicalDatasheetPath,
  canonicalCoverPath,
  moveObject,
};
