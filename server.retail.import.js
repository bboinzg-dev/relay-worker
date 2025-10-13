'use strict';

const express = require('express');
const { Transform } = require('stream');
const { Pool } = require('pg');
const QueryStream = require('pg-query-stream');
const { Storage } = require('@google-cloud/storage');
const { ProductServiceClient } = require('@google-cloud/retail').v2;

const router = express.Router();

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const storage = new Storage();
const retail = new ProductServiceClient();

const PROJECT_NUMBER = process.env.GCP_PROJECT_NUMBER;
const BRANCH = process.env.RETAIL_BRANCH ||
  (PROJECT_NUMBER
    ? `projects/${PROJECT_NUMBER}/locations/global/catalogs/default_catalog/branches/default_branch`
    : '');
const TEMP_BUCKET = process.env.GCS_TEMP_BUCKET || '';

function createObjectPath() {
  const now = new Date();
  const pad = (n, l = 2) => String(n).padStart(l, '0');
  const timestamp = `${now.getUTCFullYear()}${pad(now.getUTCMonth() + 1)}${pad(now.getUTCDate())}` +
    `${pad(now.getUTCHours())}${pad(now.getUTCMinutes())}${pad(now.getUTCSeconds())}`;
  return `retail/import/products_${timestamp}_${Date.now()}.ndjson`;
}

router.post('/api/retail/import', async (req, res, next) => {
  if (!BRANCH) {
    res.status(500).json({ done: false, error: 'RETAIL_BRANCH or GCP_PROJECT_NUMBER not configured' });
    return;
  }
  if (!TEMP_BUCKET) {
    res.status(500).json({ done: false, error: 'GCS_TEMP_BUCKET not configured' });
    return;
  }

  const dbClient = await pool.connect();
  const objectPath = createObjectPath();
  const bucket = storage.bucket(TEMP_BUCKET);
  const gcsFile = bucket.file(objectPath);
  const gcsUri = `gs://${TEMP_BUCKET}/${objectPath}`;

  try {
    const query = new QueryStream('SELECT line FROM retail.export_products_ndjson()');
    const pgStream = dbClient.query(query);

    let count = 0;
    const toNdjson = new Transform({
      objectMode: true,
      transform(row, _enc, callback) {
        count += 1;
        callback(null, `${row.line}\n`);
      },
    });

    await new Promise((resolve, reject) => {
      pgStream
        .pipe(toNdjson)
        .pipe(gcsFile.createWriteStream({
          resumable: false,
          contentType: 'application/x-ndjson',
        }))
        .on('finish', resolve)
        .on('error', reject);
    });

    const [op] = await retail.importProducts({
      parent: BRANCH,
      inputConfig: { gcsSource: { inputUris: [gcsUri] } },
      reconciliationMode: 'INCREMENTAL',
    });
    const [resp] = await op.promise();

    res.json({ done: true, result: resp, gcsUri, branch: BRANCH, count });
  } catch (err) {
    next(err);
      } finally {
    dbClient.release();
  }
});

module.exports = router;