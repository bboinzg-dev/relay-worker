const express = require('express');
const { Pool } = require('pg');

const app = express();
app.use(express.json({ limit: '10mb' }));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : false
});

app.get('/_healthz', (_req, res) => res.status(200).send('ok'));

app.post('/api/worker/ingest', async (req, res) => {
  try {
    const { gcsPdfUri } = req.body || {};
    if (!gcsPdfUri) return res.status(400).json({ ok:false, error:'gcsPdfUri required' });

    // 임시: 최소 기록만 남김 (DocAI 연동은 이후 단계에서 추가)
    await pool.query(
      `INSERT INTO public.relay_specs (gcs_pdf_uri, status, created_at, updated_at)
       VALUES ($1, 'RECEIVED', now(), now())`,
      [gcsPdfUri]
    );

    return res.json({ ok:true, accepted:true });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ ok:false, error: e.message || String(e) });
  }
});

const port = process.env.PORT || 8080;
app.listen(port, () => console.log('worker-src up on', port));
