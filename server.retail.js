'use strict';

const express = require('express');
const router = express.Router();
const { Pool } = require('pg');
const { Storage } = require('@google-cloud/storage');
const { Transform } = require('stream');
const QueryStream = require('pg-query-stream');
const { ProductServiceClient } = require('@google-cloud/retail').v2;

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const storage = new Storage();
const retail = new ProductServiceClient();

const PROJECT_NUMBER = process.env.GCP_PROJECT_NUMBER;
const BUCKET = process.env.GCS_BUCKET;
const NDJSON_PATH = process.env.RETAIL_CATALOG_OBJECT || 'retail/catalog/products.ndjson';
const GCS_URI = `gs://${BUCKET}/${NDJSON_PATH}`;
const BRANCH =
  process.env.RETAIL_BRANCH ||
  `projects/${PROJECT_NUMBER}/locations/global/catalogs/default_catalog/branches/default_branch`;

/** NDJSON Export to GCS */
router.post('/api/retail/export-catalog', async (req, res) => {
  const client = await pool.connect();
  try {
    const sql = `
      SELECT (product)::text AS line
      FROM retail.products_for_import
      ORDER BY family_slug, brand, code, id
    `;
    const query = new QueryStream(sql);
    const pgStream = client.query(query);

    const gcsFile = storage.bucket(BUCKET).file(NDJSON_PATH);
    const gcsWrite = gcsFile.createWriteStream({
      resumable: false,
      contentType: 'application/x-ndjson',
    });

    let count = 0;
    const toNdjson = new Transform({
      objectMode: true,
      transform(row, _enc, cb) {
        count++;
        cb(null, row.line + '\n');
      },
    });

    await new Promise((resolve, reject) => {
      pgStream
        .pipe(toNdjson)
        .pipe(gcsWrite)
        .on('finish', resolve)
        .on('error', reject);
    });

    const [meta] = await gcsFile.getMetadata();
    res.json({ ok: true, gcsUri: GCS_URI, count, size: meta.size });
  } catch (e) {
    console.error('[retail/export] error', e);
    res.status(500).json({ ok: false, error: e.message });
  } finally {
    client.release();
  }
});

/** Start Import (returns operation name) */
router.post('/api/retail/import-catalog', async (req, res) => {
  try {
    const [op] = await retail.importProducts({
      parent: BRANCH,
      inputConfig: { gcsSource: { inputUris: [GCS_URI] } },
      reconciliationMode: 'INCREMENTAL',
    });
    res.json({ ok: true, operation: op.name, gcsUri: GCS_URI, branch: BRANCH });
  } catch (e) {
    console.error('[retail/import] error', e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

/** Check Import Status (use ?name=operations/… ) */
router.get('/api/retail/import-catalog/status', async (req, res) => {
  try {
    const name = req.query.name;
    if (!name) return res.status(400).json({ ok: false, error: 'missing ?name' });
    const [resp] = await retail.checkImportProductsProgress(String(name));
    res.json({ ok: true, metadata: resp.metadata, done: resp.done, result: resp.result });
  } catch (e) {
    console.error('[retail/status] error', e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

module.exports = router;