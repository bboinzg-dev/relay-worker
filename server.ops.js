const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const db = require('./src/utils/db');
const { tableExists, extInstalled } = require('./src/utils/dbhelpers');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '10mb' }));

app.get('/_healthz/deep', async (req, res) => {
  const out = { ok: false, time: new Date().toISOString() };
  try {
    const v = await db.query('select version()');
    const pg = v.rows[0].version || null;
    const exts = {};
    for (const name of ['pg_trgm','vector','uuid-ossp']) {
      exts[name] = await extInstalled(name);
    }
    out.db = { ok: true, version: pg, extensions: exts };
    out.bucket = process.env.GCS_BUCKET || null;
    out.env = {
      project: process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT || null,
      region: process.env.VERTEX_LOCATION || null,
    };
    out.ok = true;
    res.json(out);
  } catch (e) {
    out.error = String(e.message || e);
    res.status(500).json(out);
  }
});

app.get('/api/ops/metrics', async (req, res) => {
  const metrics = { time: new Date().toISOString() };
  function safeCount(sql, params=[]) {
    return db.query(sql, params).then(r => Number(r.rows[0]?.c || 0)).catch(()=>null);
  }
  try {
    metrics.listings = await safeCount('SELECT count(*) c FROM public.listings');
    metrics.purchase_requests = await db.query(`SELECT status, count(*) c FROM public.purchase_requests GROUP BY status`).then(r => r.rows).catch(()=>[]);
    metrics.bids = await safeCount('SELECT count(*) c FROM public.bids');
    // notification jobs breakdown if table exists
    const hasJobs = await tableExists('public','notification_jobs');
    metrics.notification_jobs = hasJobs ? await db.query(`SELECT status, count(*) c FROM public.notification_jobs GROUP BY status`).then(r => r.rows) : null;

    // last failures
    let failures = [];
    if (hasJobs) {
      const f = await db.query(`SELECT id, event_id, webhook_url, last_error, attempt_count, updated_at FROM public.notification_jobs WHERE status='failed' ORDER BY updated_at DESC LIMIT 50`);
      failures = f.rows;
    }
    metrics.failures = { notification_jobs: failures };

    res.json(metrics);
  } catch (e) {
    res.status(500).json({ error: String(e.message || e) });
  }
});

app.get('/api/ops/failures', async (req, res) => {
  try {
    const out = {};
    if (await tableExists('public','notification_jobs')) {
      const r = await db.query(`SELECT * FROM public.notification_jobs WHERE status='failed' ORDER BY updated_at DESC LIMIT 200`);
      out.notification_jobs = r.rows;
    }
    if (await tableExists('public','ingest_jobs')) {
      const r2 = await db.query(`SELECT * FROM public.ingest_jobs WHERE status IN ('FAILED','ERROR') ORDER BY updated_at DESC LIMIT 200`);
      out.ingest_jobs = r2.rows;
    }
    res.json(out);
  } catch (e) {
    res.status(500).json({ error: String(e.message || e) });
  }
});

module.exports = app;
