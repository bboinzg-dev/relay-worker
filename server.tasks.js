const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const db = require('./src/utils/db');
const { getSignedUrl } = require('./src/utils/gcsSignedUrl');
const { ensureEventQueue, enqueueEvent } = require('./src/utils/eventQueue');
const qualityScanner = (()=>{ try { return require('./src/quality/scanner'); } catch { return null; } })();

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '8mb' }));

async function fetchBatch(limit=20){
  await ensureEventQueue();
  const r = await db.query(`
    UPDATE public.event_queue SET status='processing', attempts=attempts+1
    WHERE id IN (
      SELECT id FROM public.event_queue WHERE status='queued' AND run_at <= now() ORDER BY run_at, created_at LIMIT $1
    )
    RETURNING *;
  `, [Math.max(1, Math.min(100, limit))]);
  return r.rows;
}

async function handleOne(ev){
  const t = ev.type;
  const p = ev.payload || {};
  if (t === 'quality_scan_family') {
    if (!qualityScanner) return { ok: false, error: 'quality module missing' };
    await qualityScanner.runScan({ family: p.family_slug || null });
    return { ok: true };
  }
  if (t === 'spec_upsert') {
    // simple fan-out
    if (p.family_slug) await enqueueEvent('quality_scan_family', { family_slug: p.family_slug });
    if (!p.cover && p.datasheet_url) await enqueueEvent('cover_regen', { family_slug: p.family_slug, brand: p.brand, code: p.code, datasheet_url: p.datasheet_url });
    if (p.datasheet_url) await enqueueEvent('signed_url_warm', { gcs: p.datasheet_url });
    if (p.cover) await enqueueEvent('signed_url_warm', { gcs: p.cover });
    return { ok: true };
  }
  if (t === 'cover_regen') {
    // stub: if cover field is empty, set a conventional path. (actual rendering pipeline to be added later)
    const brand = (p.brand||'').toString(), code=(p.code||'').toString();
    const ds = (p.datasheet_url||'').toString();
    if (!brand || !code || !ds) return { ok:false, error:'missing brand/code/datasheet_url' };
    const coverBucket = process.env.GCS_BUCKET || 'partsplan-473810-docai-us';
    const cover = `gs://${coverBucket}/images/${brand.toLowerCase()}/${code.toLowerCase()}/cover.png`;
    await db.query(`UPDATE public.relay_specs SET cover=$3 WHERE lower(brand)=lower($1) AND lower(code)=lower($2) AND (cover IS NULL OR cover='')`, [brand, code, cover]);
    return { ok: true, cover };
  }
  if (t === 'signed_url_warm') {
    const gcs = (p.gcs||'').toString();
    if (!gcs) return { ok:false, error:'gcs required' };
    await getSignedUrl(gcs, { expiresSec: 1800 });
    return { ok: true };
  }
  return { ok: false, error: 'unknown event' };
}

app.post('/api/tasks/process-events', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit || '20', 10);
    const batch = await fetchBatch(limit);
    const results = [];
    for (const ev of batch) {
      try {
        const out = await handleOne(ev);
        if (out.ok) {
          await db.query(`UPDATE public.event_queue SET status='done', last_error=NULL WHERE id=$1`, [ev.id]);
        } else {
          await db.query(`UPDATE public.event_queue SET status='error', last_error=$2 WHERE id=$1`, [ev.id, out.error || 'error']);
        }
        results.push({ id: ev.id, type: ev.type, ok: out.ok, error: out.error || null });
      } catch (e) {
        await db.query(`UPDATE public.event_queue SET status='error', last_error=$2 WHERE id=$1`, [ev.id, String(e.message || e)]);
        results.push({ id: ev.id, type: ev.type, ok: false, error: String(e.message || e) });
      }
    }
    res.json({ ok: true, processed: results.length, results });
  } catch (e) {
    console.error(e); res.status(400).json({ error: String(e.message || e) });
  }
});

// Peek queue
app.get('/api/tasks/queue', async (req, res) => {
  try {
    await ensureEventQueue();
    const r = await db.query(`SELECT * FROM public.event_queue ORDER BY created_at DESC LIMIT 200`);
    res.json({ items: r.rows });
  } catch (e) { console.error(e); res.status(400).json({ error: String(e.message || e) }); }
});

module.exports = app;
