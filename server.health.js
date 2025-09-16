// server.health.js (CJS)
'use strict';
const express = require('express');
const db = require('./src/utils/db');

const router = express.Router();

// 필요한 경우 GCS_BUCKET 계산(이미 server.js에 있으면 제거 가능)
const GCS_BUCKET_URI = process.env.GCS_BUCKET || '';
const GCS_BUCKET = GCS_BUCKET_URI.startsWith('gs://')
  ? GCS_BUCKET_URI.replace(/^gs:\/\//, '').split('/')[0]
  : (GCS_BUCKET_URI || '');

router.get('/_healthz', (_req, res) => res.type('text/plain').send('ok'));

router.get('/_env', async (_req, res) => {
  try {
    const r = await db.query('SELECT 1');
    res.json({ node: process.version, has_db: r?.rowCount === 1, gcs_bucket: GCS_BUCKET ? `gs://${GCS_BUCKET}` : null });
  } catch {
    res.json({ node: process.version, has_db: false, gcs_bucket: GCS_BUCKET ? `gs://${GCS_BUCKET}` : null });
  }
});

router.get('/api/health', async (_req, res) => {
  try { await db.query('SELECT 1'); return res.json({ ok: true }); }
  catch (e) { return res.status(500).json({ ok: false, error: String(e?.message || e) }); }
});

module.exports = router;
