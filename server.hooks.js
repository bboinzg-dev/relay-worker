const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const { enqueueEvent } = require('./src/utils/eventQueue');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '8mb' }));

// Ingest pipeline should call this once per spec upsert
app.post('/api/hooks/spec-upsert', async (req, res) => {
  try {
    const { family_slug, brand, code, datasheet_url, cover } = req.body || {};
    if (!brand || !code) return res.status(400).json({ error: 'brand/code required' });

    // enqueue quality scan for the family (best-effort when family provided)
    if (family_slug) {
      await enqueueEvent('quality_scan_family', { family_slug });
    }
    // enqueue cover regen if missing
    if (!cover && (datasheet_url||'').startsWith('gs://')) {
      await enqueueEvent('cover_regen', { family_slug, brand, code, datasheet_url });
    }
    // warm signed url cache (pdf & cover)
    const warm = [];
    if (datasheet_url) warm.push(datasheet_url);
    if (cover) warm.push(cover);
    for (const gcs of warm) {
      await enqueueEvent('signed_url_warm', { gcs });
    }

    res.json({ ok: true });
  } catch (e) {
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  }
});

module.exports = app;
