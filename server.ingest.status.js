// server.ingest.status.js (새 파일)
'use strict';
const express = require('express');
const router = express.Router();
const db = require('./src/utils/db'); // 프로젝트 경로에 맞게

router.get('/api/ingest/:id', async (req, res) => {
  const id = req.params.id;
  try {
    const { rows } = await db.query('SELECT * FROM public.ingest_jobs WHERE id=$1', [id]);
    if (!rows.length) return res.status(404).json({ ok:false, error:'not_found' });
    return res.json({ ok:true, job: rows[0] });
  } catch (e) {
    console.error('[ingest status]', e);
    return res.status(500).json({ ok:false, error:'status_query_failed' });
  }
});

module.exports = router;
