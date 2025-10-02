// server.ingest.status.js
'use strict';
const express = require('express');
const router = express.Router();
const db = require('./src/utils/db');

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

router.get('/api/ingest/:key', async (req, res) => {
  const key = String(req.params.key || '').trim();
  if (!key) return res.status(400).json({ ok:false, error:'EMPTY_KEY' });

  try {
    let by = 'job_id';
    let job = null;
    let logs = [];
    let status = 'UNKNOWN';

    if (UUID_RE.test(key)) {
      by = 'run_id';
      const { rows } = await db.query(
        `select id, run_id, event, detail, ts
           from public.ingest_run_logs
          where run_id = $1
          order by ts desc
          limit 50`, [key]
      );
      logs = rows || [];
      if (logs[0]?.event) {
        const ev = String(logs[0].event).toUpperCase();
        if (ev.includes('FAILED')) status = 'FAILED';
        else if (ev.includes('SUCCEEDED') || ev.includes('DONE')) status = 'SUCCEEDED';
        else if (ev.includes('PROCESS') || ev.includes('START')) status = 'PROCESSING';
      }
    } else {
      const { rows } = await db.query(
        `select id, status, source_type, gcs_pdf_uri, last_error, created_at, updated_at
           from public.ingest_jobs
          where id = $1`, [key]
      );
      job = rows?.[0] || null;
      status = job?.status || 'UNKNOWN';
    }

    return res.json({ ok:true, by, key, status, job, logs });
  } catch (e) {
    console.error('[ingest status]', e);
    return res.status(500).json({ ok:false, error:'status_query_failed' });
  }
});

module.exports = router;
