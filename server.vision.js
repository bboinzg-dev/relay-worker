
// server.vision.js
'use strict';

const express = require('express');
const cors = require('cors');
const multer = require('multer');
const bodyParser = require('body-parser');
const { VertexAI } = require('@google-cloud/vertexai');
const db = require('./src/utils/db');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '15mb' }));
const upload = multer({ storage: multer.memoryStorage() });

const PROJECT_ID = process.env.GCP_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT;
const LOCATION   = process.env.VERTEX_LOCATION || 'asia-northeast3';
const MODEL      = process.env.GEMINI_MODEL_CLASSIFY || 'gemini-2.5-flash';

const vertex = new VertexAI({ project: PROJECT_ID, location: LOCATION });
const model  = vertex.getGenerativeModel({ model: MODEL });

// POST /api/vision/guess  (multipart: file)
app.post('/api/vision/guess', upload.single('file'), async (req, res) => {
  try {
    if (!req.file || !req.file.buffer) return res.status(400).json({ error: 'file required' });
    const bytes = req.file.buffer;
    const base64 = bytes.toString('base64');

    // Ask Gemini to extract brand/code/family guess
    const prompt = `You are helping identify electronic components from board or datasheet photos.
Return a compact JSON with keys: family_slug, brand, code, confidence (0..1), rationale (short).
If uncertain, still provide your best single guess.`;

    const resp = await model.generateContent({
      contents: [{
        role: 'user',
        parts: [
          { text: prompt },
          { inlineData: { mimeType: req.file.mimetype || 'image/png', data: base64 } }
        ]
      }]
    });
    const text = resp?.response?.candidates?.[0]?.content?.parts?.map(p => p.text).join(' ').trim() || '';
    let guess = {};
    try { guess = JSON.parse(text); } catch { guess = { raw: text }; }

    // Optional: do a nearest-neighbor by (brand,code) if present
    let nearest = [];
    const brand = (guess.brand || '').toString();
    const code  = (guess.code || '').toString();
    if (brand || code) {
      const q = `
        WITH p AS (
          SELECT lower($1) AS b, lower($2) AS c
        )
        SELECT 'relay_power_specs' AS table, brand, code, series, family_slug, datasheet_uri
          FROM public.relay_power_specs, p
         WHERE (brand_norm = p.b OR p.b = '')
            OR (code_norm = p.c  OR p.c = '')
         LIMIT 20`;
      const r = await db.query(q, [brand, code]);
      nearest = r.rows;
    }

    res.json({ ok: true, guess, nearest });
  } catch (e) {
    console.error(e);
    res.status(400).json({ ok:false, error:String(e.message || e) });
  }
});

module.exports = app;
