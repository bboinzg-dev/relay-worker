'use strict';
const path = require('node:path');
const fs = require('node:fs/promises');
const os = require('node:os');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const execFileP = promisify(execFile);

const { storage } = require('../utils/gcs');

function parseGcsUri(gcsUri) {
  const m = /^gs:\/\/([^/]+)\/(.+)$/.exec(String(gcsUri || ''));
  if (!m) throw new Error('INVALID_GCS_URI');
  return { bucket: m[1], name: m[2] };
}
function canonicalCoverPath(targetBucket, family, brand, code) {
  const norm = s => String(s||'unknown').toLowerCase().replace(/[^a-z0-9_-]+/g,'-');
  return `gs://${targetBucket}/covers/${norm(family)}/${norm(brand)}/${norm(code)}.png`;
}

async function tryExtractCover(gcsPdfUri, { family, brand, code, targetBucket }) {
  try {
    const { bucket, name } = parseGcsUri(gcsPdfUri);
    const tmp = path.join(os.tmpdir(), `cover-${Date.now()}`);
    await fs.mkdir(tmp, { recursive: true });

    const pdf = path.join(tmp, 'doc.pdf');
    const [buf] = await storage.bucket(bucket).file(name).download();
    await fs.writeFile(pdf, buf);

    await execFileP('pdfimages', ['-f','1','-l','2','-png', pdf, path.join(tmp, 'img')]); // 없으면 throw → catch(null)

    const files = (await fs.readdir(tmp)).filter(f => /^img-\d+-\d+\.png$/i.test(f));
    if (!files.length) return null;

    let pick=null, size=-1;
    for (const f of files) {
      const st = await fs.stat(path.join(tmp, f));
      if (st.size > size) { pick=f; size=st.size; }
    }
    if (!pick) return null;

    const dst = canonicalCoverPath(targetBucket, family, brand, code);
    const { bucket: db, name: dn } = parseGcsUri(dst);
    await storage.bucket(db).upload(path.join(tmp, pick), { destination: dn, resumable: false });
    return dst;
  } catch { return null; }
}

module.exports = { tryExtractCover };
