'use strict';

const express = require('express');
const { ProductServiceClient } = require('@google-cloud/retail').v2;

const router = express.Router();

const client = new ProductServiceClient();

const PROJECT_NUMBER = process.env.GCP_PROJECT_NUMBER;
const BRANCH = PROJECT_NUMBER
  ? `projects/${PROJECT_NUMBER}/locations/global/catalogs/default_catalog/branches/default_branch`
  : '';
const GCS_URI = process.env.GCS_BUCKET
  ? `gs://${process.env.GCS_BUCKET}/retail/catalog/products.ndjson`
  : '';

router.post('/api/retail/import', async (req, res, next) => {
  try {
    if (!BRANCH) {
      res.status(500).json({ done: false, error: 'GCP_PROJECT_NUMBER not configured' });
      return;
    }
    if (!GCS_URI) {
      res.status(500).json({ done: false, error: 'GCS_BUCKET not configured' });
      return;
    }

    const [op] = await client.importProducts({
      parent: BRANCH,
      inputConfig: { gcsSource: { inputUris: [GCS_URI] } },
      reconciliationMode: 'INCREMENTAL',
    });
    const [resp] = await op.promise();
    res.json({ done: true, result: resp });
  } catch (err) {
    next(err);
  }
});

module.exports = router;