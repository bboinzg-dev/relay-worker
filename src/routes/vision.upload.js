'use strict';

const express = require('express');
const multer = require('multer');
const crypto = require('crypto');
const { Storage } = require('@google-cloud/storage');
const { GoogleAuth } = require('google-auth-library');
const { safeJsonParse } = require('../utils/safe-json');

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
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: MAX_PHOTO } });

// ---------- CORS ----------
router.use('/api/vision/guess', (req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', ALLOW_ORIGIN);
  res.setHeader('Vary', 'Origin');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-api-key');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(204).end();
  next();
});

function ext(name) {
  const s = String(name || '');
  const i = s.lastIndexOf('.');
  if (i < 0) return '';
  const e = s.slice(i + 1).toLowerCase();
  return e && e.length <= 5 ? '.' + e : '';
}

function fallbackBrand(raw) {
  const map = {
    panasonic: "Panasonic", omron: "Omron", infineon: "Infineon", abb: "ABB",
    semikron: "Semikron", "mitsubishi electric": "Mitsubishi Electric", mitsubishi: "Mitsubishi Electric",
    onsemi: "onsemi", stmicro: "STMicroelectronics", "st micro": "STMicroelectronics",
    nec: "NEC", toshiba: "Toshiba", "te connectivity": "TE Connectivity", tyco: "TE Connectivity",
    ixys: "IXYS", songle: "Songle", hongfa: "Hongfa", panasonicelectric: "Panasonic"
  };
  const key = Object.keys(map).find(k => new RegExp(`\\b${k}\\b`, 'i').test(raw || ''));
  return key ? map[key] : undefined;
}
function fallbackCode(raw) {
  const m = (raw || '').match(/\b([A-Z]{1,6}[A-Z0-9\-]{3,})\b/g);
  if (!m) return undefined;
  const pick = m.find(t => !/^MODEL|MODULE|RELAY|POWER|IGBT|MOSFET|CAPACITOR$/i.test(t));
  return pick || m[0];
}

// 업로드 + 분석
router.post('/api/vision/guess', upload.any(), async (req, res) => {
  try {
    if (API_KEY && req.get('x-api-key') !== API_KEY) {
      return res.status(401).json({ ok: false, error: 'invalid api key' });
    }

    // file | image | 첫 번째 파일 모두 허용
    const files = Array.isArray(req.files) ? req.files : [];
    const f = files.find(fi => fi.fieldname === 'image' || fi.fieldname === 'file') || files[0];
    console.log('[vision.upload] fields:', files.map(x => x.fieldname));
    if (!f) return res.status(400).json({ ok: false, error: 'multipart file field required (image or file)' });

    // ---------- 1) GCS 저장 ----------
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

    // ---------- 2) Vertex 호출 (responseSchema 강제) ----------
    const auth = new GoogleAuth({ scopes: ['https://www.googleapis.com/auth/cloud-platform'] });
    const client = await auth.getClient();
    const token = (await client.getAccessToken()).token;
    if (!token) throw new Error('cannot get access token');

    const endpoint = `https://${LOCATION}-aiplatform.googleapis.com/v1/projects/${PROJECT_ID}/locations/${LOCATION}/publishers/google/models/${MODEL_ID}:generateContent`;
    const prompt = [
      "You are an electronics sourcing assistant.",
      "Return ONLY JSON (no commentary).",
      "Required keys:",
      '{ "brand":string, "code":string, "family":string, "score":number,',
      '  "presentation":{ "familyDisplay":string, "usage":string, "mainSpecs":[{"name":string,"value":string}] } }',
      "Use manufacturer-canonical display names and part numbers.",
      'Keep values short (e.g., "Uc 275V", "Imax 40kA"). If unsure, best-effort guess (do NOT omit keys).',
    ].join('\n');

    const base64 = f.buffer.toString('base64');
    const vr = await fetch(endpoint, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        contents: [{
          role: 'user',
          parts: [{ text: prompt }, { inlineData: { mimeType: f.mimetype || 'image/jpeg', data: base64 } }]
        }],
        generationConfig: {
          temperature: 0.2,
          topP: 0.8,
          responseMimeType: 'application/json',
          responseSchema: {
            type: "object",
            properties: {
              brand: { type: "string" },
              code:  { type: "string" },
              family:{ type: "string" },
              score: { type: "number" },
              presentation: {
                type: "object",
                properties: {
                  familyDisplay: { type: "string" },
                  usage:         { type: "string" },
                  mainSpecs: {
                    type: "array",
                    items: { type: "object",
                      properties: { name:{type:"string"}, value:{type:"string"} } }
                  }
                }
              }
            }
          }
        }
      }),
    });

    const vtext = await vr.text();
    if (!vr.ok) return res.status(502).json({ ok: false, error: `vertex ${vr.status}: ${vtext.slice(0, 200)}` });

    let body = {}; try { body = safeJsonParse(vtext) || {}; } catch {}
    const parts = body?.candidates?.[0]?.content?.parts || [];
    const rawText = parts.map(p => p?.text || '').join('\n');
    let out = {}; try { out = rawText ? safeJsonParse(rawText) || {} : {}; } catch {}

    // ---------- 3) 폴백(누락 방지) ----------
    let brand  = (out.brand  || '').trim();
    let code   = (out.code   || '').trim();
    const family = (out.family || out.familySlug || '').trim();
    let score  = typeof out.score === 'number' ? out.score : NaN;

    if (!brand && rawText) brand = fallbackBrand(rawText) || '';
    if (!code  && rawText) code  = fallbackCode(rawText)  || '';
    if (!(score >= 0)) score = (brand && code) ? 0.85 : (brand || code) ? 0.6 : 0.0;

    const pres = out.presentation || {};
    const mainSpecs = Array.isArray(pres.mainSpecs) ? pres.mainSpecs.slice(0, 5)
                    : Array.isArray(out.mainSpecs) ? out.mainSpecs.slice(0, 5) : [];
    const presentation = {
      source: 'ai',
      normalizedBrand: brand,
      normalizedCode : code,
      familySlug     : family,
      familyDisplayName: pres.familyDisplay || out.familyDisplay || family,
      usageShort     : pres.usage || out.usage || '',
      mainSpecs,
      confidence     : score,
      notes          : 'AI normalized',
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
