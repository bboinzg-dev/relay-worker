'use strict';
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const path = require('node:path');
const fs = require('node:fs/promises');
const { Storage } = require('@google-cloud/storage');

const execFileP = promisify(execFile);
const storage = new Storage();

function parseGcsUri(uri) {
  const m = uri.match(/^gs:\/\/([^/]+)\/(.+)$/);
  if (!m) throw new Error('invalid gcs uri: '+uri);
  return { bucket: m[1], name: m[2] };
}
function coverDstGcs({ family, brand, code }) {
  const bucket = process.env.ASSET_BUCKET || process.env.GCS_BUCKET;
  const safe = s => String(s||'unknown').toLowerCase().replace(/[^a-z0-9._-]/g,'-');
  return `gs://${bucket}/covers/${safe(family)}/${safe(brand)}/${safe(code)}.png`;
}

// 첫 1~2페이지에서 가장 큰 이미지를 뽑아 GCS에 png로 업로드
async function extractCoverToGcs(gcsPdfUri, { family, brand, code }) {
  const { bucket, name } = parseGcsUri(gcsPdfUri);
  const tmpDir = path.join('/tmp', 'pdfimg-'+Date.now());
  await fs.mkdir(tmpDir, { recursive: true });

  const [buf] = await storage.bucket(bucket).file(name).download();
  const localPdf = path.join(tmpDir, 'doc.pdf');
  await fs.writeFile(localPdf, buf);

  await execFileP('pdfimages', ['-f', '1', '-l', '2', '-png', localPdf, path.join(tmpDir, 'img')]);
  const files = (await fs.readdir(tmpDir)).filter(f => f.startsWith('img-') && f.endsWith('.png'));
  if (!files.length) return null;

  let best = null, bestSize = -1;
  for (const f of files) {
    const stat = await fs.stat(path.join(tmpDir, f));
    if (stat.size > bestSize) { best = f; bestSize = stat.size; }
  }
  if (!best) return null;

  const dst = coverDstGcs({ family, brand, code });
  const { bucket: db, name: dn } = parseGcsUri(dst);
  await storage.bucket(db).upload(path.join(tmpDir, best), { destination: dn, metadata: { contentType: 'image/png' } });
  return dst;
}

module.exports = { extractCoverToGcs };
