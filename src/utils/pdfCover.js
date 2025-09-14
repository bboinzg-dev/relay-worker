const { Storage } = require('@google-cloud/storage');
const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');
const os = require('os');

const storage = new Storage();

function parseGcs(uri){
  if (!uri || !uri.startsWith('gs://')) throw new Error('invalid gcs uri');
  const s = uri.slice(5);
  const i = s.indexOf('/');
  if (i < 0) throw new Error('invalid gcs uri');
  return { bucket: s.slice(0,i), name: s.slice(i+1) };
}

async function download(gcsUri, dst){
  const { bucket, name } = parseGcs(gcsUri);
  await storage.bucket(bucket).file(name).download({ destination: dst });
  return dst;
}

function runPdftoppm(pdfPath, outBasePath){
  return new Promise((resolve, reject)=>{
    const args = ['-png', '-singlefile', '-f', '1', '-l', '1', pdfPath, outBasePath];
    const p = spawn('pdftoppm', args, { stdio: ['ignore','pipe','pipe'] });
    let err = '';
    p.stderr.on('data', d => { err += d.toString(); });
    p.on('close', code => {
      if (code === 0) return resolve(outBasePath + '.png');
      reject(new Error('pdftoppm failed: ' + code + ' ' + err));
    });
  });
}

async function upload(srcFile, gcsUri, { cacheControl='public, max-age=86400' } = {}){
  const { bucket, name } = parseGcs(gcsUri);
  await storage.bucket(bucket).upload(srcFile, { destination: name, contentType: 'image/png', metadata: { cacheControl } });
}

async function renderCoverForPart({ gcsPdfUri, brand, code, outBucket }){
  if (!brand || !code) throw new Error('brand/code required');
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'thumb-'));
  const inPdf = path.join(tmp, 'input.pdf');
  const outBase = path.join(tmp, 'cover');
  await download(gcsPdfUri, inPdf);
  const outPng = await runPdftoppm(inPdf, outBase);
  const bucket = outBucket || process.env.GCS_BUCKET;
  if (!bucket) throw new Error('set GCS_BUCKET');
  const outGcs = `gs://${bucket}/images/${brand.toLowerCase()}/${code.toLowerCase()}/cover.png`;
  await upload(outPng, outGcs);
  return outGcs;
}

module.exports = { renderCoverForPart };
