const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const db = require('./src/utils/db');
const { renderCoverForPart } = require('./src/utils/pdfCover');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '8mb' }));

app.post('/api/render/cover', async (req, res) => {
  try {
    const { brand, code, gcsPdfUri, outBucket } = req.body || {};
    if (!brand || !code || !gcsPdfUri) return res.status(400).json({ error: 'brand, code, gcsPdfUri required' });
    const out = await renderCoverForPart({ gcsPdfUri, brand, code, outBucket });
    // persist to DB if field exists
    try {
      await db.query(`UPDATE public.relay_specs SET cover=$3 WHERE lower(brand)=lower($1) AND lower(code)=lower($2)`, [brand, code, out]);
    } catch {}
    res.json({ ok: true, cover: out });
  } catch (e) {
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  }
});

module.exports = app;
