const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const db = require('./src/utils/db');
const { ensureNotificationJobs, markJob } = require('./src/utils/notify');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '10mb' }));

async function deliverWebhook(url, body, { timeoutMs=3000 } = {}) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const resp = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
      signal: ctrl.signal,
    });
    clearTimeout(timer);
    const txt = await resp.text().catch(()=>'');
    if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${txt}`);
    return { ok: true, status: resp.status, body: txt };
  } catch (e) {
    clearTimeout(timer);
    return { ok: false, error: String(e.message || e) };
  }
}

// Cloud Tasks handler: POST /_tasks/notify { job_id }
// On success -> 200 (Cloud Tasks stops). On failure -> 500 (Cloud Tasks will retry per queue policy).
app.post('/_tasks/notify', async (req, res) => {
  try {
    await ensureNotificationJobs();
    const job_id = req.body?.job_id;
    if (!job_id) return res.status(400).json({ error: 'job_id required' });
    const r = await db.query(`SELECT * FROM public.notification_jobs WHERE id=$1`, [job_id]);
    if (!r.rows.length) return res.status(404).json({ error: 'job not found' });
    const job = r.rows[0];

    if (job.status === 'delivered') {
      return res.json({ ok: true, message: 'already delivered' });
    }

    const payload = job.payload || {};
    const out = await deliverWebhook(job.webhook_url, payload, { timeoutMs: Number(process.env.NOTIFY_WEBHOOK_TIMEOUT_MS || 3000) });

    if (out.ok) {
      await markJob(job_id, { status: 'delivered', attempt_count: job.attempt_count + 1, last_error: null, delivered_at: new Date().toISOString() });
      return res.json({ ok: true, result: out });
    } else {
      await markJob(job_id, { status: 'failed', attempt_count: job.attempt_count + 1, last_error: out.error });
      // return 500 to trigger Cloud Tasks retry with backoff
      return res.status(500).json({ error: out.error });
    }
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: String(e.message || e) });
  }
});

// Admin: list/requeue
app.get('/admin/notifications', async (req, res) => {
  const status = (req.query.status || '').toString();
  const params = []; let where = '';
  if (status) { params.push(status); where = 'WHERE status=$1'; }
  const r = await db.query(`SELECT * FROM public.notification_jobs ${where} ORDER BY created_at DESC LIMIT 500`, params);
  res.json({ items: r.rows });
});

app.post('/admin/notifications/requeue', async (req, res) => {
  try {
    const ids = Array.isArray(req.body?.ids) ? req.body.ids : [];
    const r = [];
    for (const id of ids) {
      await markJob(id, { status: 'queued' });
      const { enqueueNotify } = require('./src/utils/tasks');
      const name = await enqueueNotify(id, {});
      r.push({ id, task: name });
    }
    res.json({ ok: true, items: r });
  } catch (e) {
    res.status(400).json({ error: String(e.message || e) });
  }
});

module.exports = app;
