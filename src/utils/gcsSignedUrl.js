const { Storage } = require('@google-cloud/storage');
const db = require('../../db');
const { TTLCache } = require('./memcache');

const storage = new Storage();
const cache = new TTLCache(1000);

function parseGcsUri(uri){
  // gs://bucket/path/to/file
  if (!uri || !uri.startsWith('gs://')) return null;
  const s = uri.slice(5);
  const idx = s.indexOf('/');
  if (idx < 0) return null;
  const bucket = s.slice(0, idx);
  const name = s.slice(idx+1);
  return { bucket, name };
}

async function getSignedUrl(gcsUri, { expiresSec=1200, contentType=null } = {}){
  if (!gcsUri) throw new Error('gcsUri required');
  const inMem = cache.get(gcsUri);
  if (inMem) return inMem;

  // DB cache
  const now = new Date();
  const r = await db.query(`SELECT url, expires_at, content_type FROM public.signed_url_cache WHERE gcs_uri=$1`, [gcsUri]);
  if (r.rows.length) {
    const row = r.rows[0];
    const exp = new Date(row.expires_at);
    if (exp > now) {
      const left = exp.getTime() - now.getTime();
      cache.set(gcsUri, { url: row.url, expires_at: exp.toISOString(), gcs: gcsUri, content_type: row.content_type }, left);
      return { url: row.url, expires_at: exp.toISOString(), gcs: gcsUri, content_type: row.content_type };
    }
  }

  const parsed = parseGcsUri(gcsUri);
  if (!parsed) throw new Error('invalid gcsUri');
  const opts = { version: 'v4', action: 'read', expires: Date.now() + expiresSec * 1000 };
  if (contentType) opts.contentType = contentType;
  const [url] = await storage.bucket(parsed.bucket).file(parsed.name).getSignedUrl(opts);
  const expires_at = new Date(Date.now() + expiresSec * 1000).toISOString();
  await db.query(`INSERT INTO public.signed_url_cache (gcs_uri, url, expires_at, content_type)
                  VALUES ($1,$2,$3,$4)
                  ON CONFLICT (gcs_uri) DO UPDATE SET url=EXCLUDED.url, expires_at=EXCLUDED.expires_at, content_type=EXCLUDED.content_type`,
                  [gcsUri, url, expires_at, contentType]);
  cache.set(gcsUri, { url, expires_at, gcs: gcsUri, content_type: contentType }, expiresSec*1000);
  return { url, expires_at, gcs: gcsUri, content_type: contentType };
}

module.exports = { getSignedUrl, parseGcsUri };
