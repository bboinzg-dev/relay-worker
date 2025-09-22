'use strict';

const express = require('express');
const multer = require('multer');
const crypto = require('crypto');
const { Storage } = require('@google-cloud/storage');
const { GoogleAuth } = require('google-auth-library');

const router = express.Router();

// ==== ENV ====
const PROJECT_ID = process.env.GCP_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT || 'partsplan';
const LOCATION   = process.env.VERTEX_LOCATION || 'asia-northeast3';
const MODEL_ID   = process.env.GEMINI_MODEL_EXTRACT || process.env.VERTEX_MODEL_ID || 'gemini-2.5-flash';
const BUCKET     = process.env.GCS_BUCKET || 'partsplan-docai-us';
const MAX_PHOTO  = +(process.env.MAX_PHOTO_SIZE || 12 * 1024 * 1024);
const ALLOW_ORIGIN = process.env.ALLOW_ORIGIN || '*';
const API_KEY    = process.env.INGEST_API_KEY || '';

const storage = new Storage();
// 🔧 어떤 필드명으로 오든 받기 위해 any() 사용
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: MAX_PHOTO } });

// CORS
router.use('/api/vision/guess', (req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', ALLOW_ORIGIN);
  res.setHeader('Vary', 'Origin');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-api-key');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(204).end();
  next();
});

function ext(name) {
  const s = String(name || ''); const i = s.lastIndexOf('.'); if (i < 0) return '';
  const e = s.slice(i + 1).toLowerCase(); return e && e.length <= 5 ? '.' + e : '';
}

// 업로드 + 분석 (⚠️ 단일 라우트만 존재하도록 보장)
router.post('/api/vision/guess', upload.any(), async (req, res) => {
  try {
    if (API_KEY && req.get('x-api-key') !== API_KEY) {
      return res.status(401).json({ ok: false, error: 'invalid api key' });
    }

    // file | image | 그 외 첫 번째 파일 모두 허용
    const files = Array.isArray(req.files) ? req.files : [];
    const f = files.find(fi => fi.fieldname === 'image' || fi.fieldname === 'file') || files[0];

    if (!f) return res.status(400).json({ ok: false, error: 'multipart file field required (image or file)' });

    // 1) GCS 저장
    const now = new Date();
    const y = String(now.getUTCFullYear());
    const m = String(now.getUTCMonth() + 1).padStart(2, '0');
    const d = String(now.getUTCDate()).padStart(2, '0');
    const id = crypto.randomUUID();
    const object = `uploads/photo/${y}/${m}/${d}/${id}${ext(f.originalname)}`;

    await storage.bucket(BUCKET).file(object).save(f.buffer, {
      contentType: f.mimetype || 'application/octet-stream',
      resumable: false,
      metadata: { cacheControl: 'public, max-age=31536000' },
    });
    const gcsUri = `gs://${BUCKET}/${object}`;

    // 2) Vertex 호출 (ADC)
    const auth = new GoogleAuth({ scopes: ['https://www.googleapis.com/auth/cloud-platform'] });
    const client = await auth.getClient();
    const token = (await client.getAccessToken()).token;
    if (!token) throw new Error('cannot get access token');

    const endpoint = `https://${LOCATION}-aiplatform.googleapis.com/v1/projects/${PROJECT_ID}/locations/${LOCATION}/publishers/google/models/${MODEL_ID}:generateContent`;

    const prompt = [
      'You are an electronics sourcing assistant.',
      'From the product photo, read labels/markings and return compact JSON:',
      '{ brand, code, family, score, presentation:{ familyDisplay, usage, mainSpecs:[{name,value}]<=5 } }',
      'Values must be short (e.g., "Uc 275V", "Imax 40kA"). Omit unknown fields.'
    ].join('\n');

    const base64 = f.buffer.toString('base64');
    const vr = await fetch(endpoint, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        contents: [{ role: 'user', parts: [{ text: prompt }, { inlineData: { mimeType: f.mimetype || 'image/jpeg', data: base64 } }] }],
        generationConfig: { temperature: 0.2, responseMimeType: 'application/json' }
      }),
    });

    const vtext = await vr.text();
    if (!vr.ok) return res.status(502).json({ ok: false, error: `vertex ${vr.status}: ${vtext.slice(0, 200)}` });

    let out = {};
    try {
      const body = JSON.parse(vtext);
      const payload = body?.candidates?.[0]?.content?.parts?.[0]?.text;
      out = payload ? JSON.parse(payload) : {};
    } catch {}

    const brand  = out.brand  || '';
    const code   = out.code   || '';
    const family = out.family || out.familySlug || '';
    const score  = typeof out.score === 'number' ? out.score : 0;

    const presentation = out.presentation || {
      source: 'ai',
      normalizedBrand: brand,
      normalizedCode : code,
      familySlug     : family,
      familyDisplayName: out.familyDisplay || family,
      usageShort     : out.usage || '',
      mainSpecs      : Array.isArray(out.mainSpecs) ? out.mainSpecs.slice(0, 5) : [],
      confidence     : score,
      notes          : 'AI 추정값',
    };

    return res.json({
      ok: true,
      mode: 'guess',
      photo: { gcs_uri: gcsUri, bucket: BUCKET, object, mime: f.mimetype, size: f.size },
      brand, code, family, score,
      presentation,
    });
  } catch (err) {
    console.error('[vision.upload] error:', err);
    return res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

module.exports = router;
