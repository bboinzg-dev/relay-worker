'use strict';

const express = require('express');
const multer = require('multer');
const crypto = require('crypto');
const { Storage } = require('@google-cloud/storage');
const { VertexAI } = require('@google-cloud/vertexai');

const router = express.Router();

// ---- ENV ----
const PROJECT_ID   = process.env.GCP_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT;
const LOCATION     = process.env.VERTEX_LOCATION || 'asia-northeast3';
const MODEL_ID     = process.env.GEMINI_MODEL_ID || 'gemini-2.5-flash';
const BUCKET       = process.env.ASSET_BUCKET || process.env.UPLOAD_BUCKET || process.env.GCS_BUCKET;
const MAX_PHOTO    = +(process.env.MAX_PHOTO_SIZE || 12 * 1024 * 1024); // 12MB
const ALLOW_ORIGIN = process.env.ALLOW_ORIGIN || '*';                   // 프론트 도메인 or '*'
const API_KEY      = process.env.INGEST_API_KEY || '';                  // 선택: 간단 API 키 보호

if (!PROJECT_ID) console.warn('[vision.upload] GCP_PROJECT_ID is not set (Cloud Run default ADC will be used).');
if (!BUCKET)     console.warn('[vision.upload] ASSET_BUCKET/UPLOAD_BUCKET/GCS_BUCKET is not set!');

const storage = new Storage();
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: MAX_PHOTO } });

// --- CORS ---
function cors(req, res, next) {
  res.setHeader('Access-Control-Allow-Origin', ALLOW_ORIGIN);
  res.setHeader('Vary', 'Origin');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-api-key');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(204).end();
  next();
}
router.use('/api/vision/guess', cors);

// --- Helper ---
function extFromName(name) {
  const s = String(name || '');
  const dot = s.lastIndexOf('.');
  if (dot === -1) return '';
  const ext = s.slice(dot + 1).toLowerCase();
  return ext && ext.length <= 5 ? '.' + ext : '';
}

// ---- 업로드 + 분석 ----
router.post('/api/vision/guess', upload.single('file'), async (req, res) => {
  try {
    if (API_KEY && req.get('x-api-key') !== API_KEY) {
      return res.status(401).json({ ok: false, error: 'invalid api key' });
    }
    if (!req.file) return res.status(400).json({ ok: false, error: 'file is required (multipart field "file")' });
    if (!BUCKET)   return res.status(500).json({ ok: false, error: 'ASSET_BUCKET env is required' });

    // 1) GCS 저장
    const now = new Date();
    const y = String(now.getUTCFullYear());
    const m = String(now.getUTCMonth() + 1).padStart(2, '0');
    const d = String(now.getUTCDate()).padStart(2, '0');
    const id = crypto.randomUUID();
    const ext = extFromName(req.file.originalname);
    const objectPath = `uploads/photo/${y}/${m}/${d}/${id}${ext}`;

    const gcsFile = storage.bucket(BUCKET).file(objectPath);
    await gcsFile.save(req.file.buffer, {
      contentType: req.file.mimetype || 'application/octet-stream',
      resumable:   false,
      metadata: { cacheControl: 'public, max-age=31536000' },
    });

    const gcsUri = `gs://${BUCKET}/${objectPath}`;

    // 2) Vertex 분석 (ADC로 서비스계정 권한 사용)
    const vertex = new VertexAI({ project: PROJECT_ID, location: LOCATION });
    const model  = vertex.getGenerativeModel({
      model: MODEL_ID,
      generationConfig: { temperature: 0.2, responseMimeType: 'application/json' },
    });

    const prompt = [
      'You are an electronics sourcing assistant.',
      'From the provided product photo, read the label/marking and output compact JSON with keys:',
      '- brand (normalized), code (normalized), family (slug like relay_power, mov_varistor, resistor_chip, tvs_diode, capacitor_mlcc, etc.),',
      '- score (0..1 confidence),',
      '- presentation { familyDisplay, usage, mainSpecs:[{name,value}] (<=5) }',
      'Keep values short (e.g., "Uc 275V", "Imax 40kA"). Omit unknown fields.',
    ].join('\n');

    const base64 = req.file.buffer.toString('base64');
    const out = await model.generateContent({
      contents: [
        { role: 'user',
          parts: [
            { text: prompt },
            { inlineData: { mimeType: req.file.mimetype || 'image/jpeg', data: base64 } },
          ],
        },
      ],
    });

    let parsed = {};
    try {
      parsed = JSON.parse(out.response?.candidates?.[0]?.content?.parts?.[0]?.text || '{}');
    } catch { /* ignore */ }

    const brand   = parsed.brand  || '';
    const code    = parsed.code   || '';
    const family  = parsed.family || parsed.familySlug || '';
    const score   = typeof parsed.score === 'number' ? parsed.score : 0;

    const presentation = parsed.presentation || {
      source: 'ai',
      normalizedBrand: brand,
      normalizedCode : code,
      familySlug     : family,
      familyDisplayName: parsed.familyDisplay || family,
      usageShort     : parsed.usage || '',
      mainSpecs      : Array.isArray(parsed.mainSpecs) ? parsed.mainSpecs.slice(0, 5) : [],
      confidence     : score,
      notes          : 'AI 추정값',
    };

    return res.json({
      ok: true,
      mode: 'guess',
      photo: { gcs_uri: gcsUri, bucket: BUCKET, object: objectPath, mime: req.file.mimetype, size: req.file.size },
      brand, code, family, score,
      presentation,
    });
  } catch (err) {
    console.error('[vision/guess] error:', err);
    return res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

module.exports = router;
