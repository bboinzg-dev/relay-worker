const { Storage } = require('@google-cloud/storage');

const storage = new Storage();

function parseGcsUri(gcsUri) {
  if (!gcsUri?.startsWith('gs://')) throw new Error('gcsUri must start with gs://');
  const [, , bucket, ...rest] = gcsUri.split('/');
  return { bucket, name: rest.join('/') };
}

async function readText(gcsUri, limitBytes=4*1024*1024) {
  const { bucket, name } = parseGcsUri(gcsUri);
  const [buf] = await storage.bucket(bucket).file(name).download({ start: 0, end: limitBytes });
  return buf.toString('utf8');
}

async function readBytes(gcsUri, limitBytes=10*1024*1024) {
  const { bucket, name } = parseGcsUri(gcsUri);
  const [buf] = await storage.bucket(bucket).file(name).download({ start: 0, end: limitBytes });
  return buf;
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

function canonicalDatasheetPath(bucket, family, brand, code) {
  const b = (brand||'').toLowerCase().replace(/[^a-z0-9._-]/g, '-');
  const c = (code||'').toLowerCase().replace(/[^a-z0-9._-]/g, '-');
  const f = (family||'').toLowerCase().replace(/[^a-z0-9._-]/g, '-');
  return `gs://${bucket}/datasheets/${f}/${b}/${c}.pdf`;
}

function canonicalCoverPath(bucket, family, brand, code) {
  const b = (brand||'').toLowerCase().replace(/[^a-z0-9._-]/g, '-');
  const c = (code||'').toLowerCase().replace(/[^a-z0-9._-]/g, '-');
  const f = (family||'').toLowerCase().replace(/[^a-z0-9._-]/g, '-');
  return `gs://${bucket}/images/${f}/${b}/${c}/cover.png`;
}

async function moveObject(srcGcsUri, dstGcsUri) {
  const src = parseGcsUri(srcGcsUri);
  const dst = parseGcsUri(dstGcsUri);
  await storage.bucket(src.bucket).file(src.name).move(storage.bucket(dst.bucket).file(dst.name));
  return dstGcsUri;
}

module.exports = { storage, parseGcsUri, readText, readBytes, getSignedUrl, canonicalDatasheetPath, canonicalCoverPath, moveObject };
